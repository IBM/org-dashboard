// Fetchers for issue & pull request data and metrics

import {
  IssueConnection,
  PageInfo,
  PullRequestConnection,
  Repository,
  RepositoryConnection,
} from '@octokit/graphql-schema';
import { Config, Fetcher } from '..';
import { checkRateLimit, CustomOctokit } from '../lib/octokit';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Waits until the primary rate limit has enough headroom.
 * Called before each parallel batch so we don't start a batch we can't finish.
 */
const throttleForRateLimit = async (
  octokit: CustomOctokit,
  minRemaining = 100,
) => {
  const { remaining, reset } = await checkRateLimit(octokit);
  if (remaining <= minRemaining) {
    const waitMs = reset * 1000 - Date.now() + 5_000; // wait until reset + 5s buffer
    console.log(
      `⏳  Rate limit low (${remaining} remaining) — waiting ${Math.round(waitMs / 1000)}s for reset`,
    );
    await sleep(waitMs > 0 ? waitMs : 5_000);
  }
};

/**
 * Runs tasks with a fixed concurrency limit.
 * Processes `tasks` in chunks of `concurrency` at a time.
 */
const runWithConcurrency = async <T>(
  tasks: (() => Promise<T>)[],
  concurrency: number,
): Promise<T[]> => {
  const results: T[] = [];
  for (let i = 0; i < tasks.length; i += concurrency) {
    const batch = tasks.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map((t) => t()));
    results.push(...batchResults);
  }
  return results;
};

const getIssueAndPrData = async (octokit: CustomOctokit, config: Config) => {
  const issueData = await octokit.graphql.paginate<{
    organization: {
      repositories: {
        totalCount: RepositoryConnection['totalCount'];
        pageInfo: PageInfo;
        nodes: {
          name: Repository['name'];
          totalIssues: IssueConnection;
          openIssues: IssueConnection;
          closedIssues: IssueConnection;
          totalPullRequests: PullRequestConnection;
          openPullRequests: PullRequestConnection;
          closedPullRequests: PullRequestConnection;
          mergedPullRequests: PullRequestConnection;
        }[];
      };
    };
  }>(
    `
  query($cursor: String, $organization: String!) {
    organization(login:$organization){
      repositories(privacy:PUBLIC, first:25, isFork:false, isArchived:false, after: $cursor) {
        totalCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          name
          totalIssues: issues {
            totalCount
          }
          closedIssues: issues(states:CLOSED) {
            totalCount
          }
          openIssues: issues(states:OPEN) {
            totalCount
          }
          openPullRequests: pullRequests(states:OPEN) {
            totalCount
          }
          totalPullRequests: pullRequests {
            totalCount
          }
          closedPullRequests: pullRequests(states:CLOSED) {
            totalCount
          }
          mergedPullRequests: pullRequests(states:MERGED) {
            totalCount
          }
        }
      }
    }
  }
`,
    {
      organization: config.organization,
    },
  );

  return issueData;
};

export const addIssueAndPrData: Fetcher = async (result, octokit, config) => {
  const dataResult = await getIssueAndPrData(octokit, config);
  dataResult.organization.repositories.nodes.forEach((repo) => {
    result.repositories[repo.name] = {
      ...result.repositories[repo.name],
      totalIssuesCount: repo.totalIssues.totalCount,
      openIssuesCount: repo.openIssues.totalCount,
      closedIssuesCount: repo.closedIssues.totalCount,
      totalPullRequestsCount: repo.totalPullRequests.totalCount,
      openPullRequestsCount: repo.openPullRequests.totalCount,
      closedPullRequestsCount: repo.closedPullRequests.totalCount,
      mergedPullRequestsCount: repo.mergedPullRequests.totalCount,
    };
  });

  return result;
};

const calculateIssueMetricsPerRepo = async (
  repoName: string,
  state: 'open' | 'closed',
  octokit: CustomOctokit,
  config: Config,
) => {
  const result = await octokit.paginate(octokit.issues.listForRepo, {
    owner: config.organization,
    repo: repoName,
    state: state,
    // Need to limit this query somehow, otherwise it will take forever/timeout
    since: config.since,
  });

  // Calculate the total age of open issues
  const issues = result.filter((issue) => !issue.pull_request);
  const issuesCount = issues.length;
  const issuesTotalAge = issues.reduce((acc, issue) => {
    const createdAt = new Date(issue.created_at);
    const now = new Date();
    const age = now.getTime() - createdAt.getTime();
    return acc + age;
  }, 0);

  // Calculate the age of open issues
  const issuesAverageAge = issuesCount > 0 ? issuesTotalAge / issuesCount : 0;
  const issuesMedianAge =
    issues.length > 0
      ? new Date().getTime() -
        new Date(issues[Math.floor(issues.length / 2)].created_at).getTime()
      : 0;

  return {
    issuesCount,
    issuesAverageAge,
    issuesMedianAge,
  };
};

const calculateIssueResponseTime = async (
  repoName: string,
  octokit: CustomOctokit,
  config: Config,
) => {
  const result = await octokit.graphql.paginate<{ repository: Repository }>(
    `
    query ($cursor: String, $organization: String!, $repoName: String!, $since: DateTime!) {
      repository(owner: $organization, name:$repoName) {
        issues(first: 100, after: $cursor, filterBy: {since: $since}) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            author {
              login
            }
            createdAt
            comments(first: 30) {
              totalCount
              nodes {
                createdAt
                author {
                  __typename
                  login
                  ... on Bot {
                    id
                  }
                }
                isMinimized
              }
            }
          }
        }
      }
    }
  `,
    {
      organization: config.organization,
      repoName: repoName,
      since: config.since,
    },
  );

  // Check if there are any issues at all
  if (
    !result.repository ||
    !result.repository.issues.nodes ||
    result.repository.issues.nodes?.length === 0
  ) {
    return {
      issuesCount: 0,
      issuesResponseAverageAge: 0,
      issuesResponseMedianAge: 0,
    };
  }

  // Filter out issues without comments that meet our criteria
  // Criteria:
  // - not the author of the issue
  // - the comment is not a bot
  // - the comment is not marked as spam
  //
  // Also filter out issues without comments after we filtered the comments
  const issues = result.repository.issues.nodes
    .map((issue) => {
      return {
        ...issue,
        comments: {
          nodes: issue!.comments.nodes?.filter(
            (comment) =>
              comment!.author?.login !== issue!.author?.login &&
              comment!.author?.__typename !== 'Bot' &&
              !comment?.isMinimized,
          ),
        },
      };
    })
    .filter((issue) => issue!.comments?.nodes?.length ?? 0 > 0);

  const issuesCount = issues.length;

  // Calculate the response time for each issue
  const issuesResponseTime = issues.map((issue) => {
    const createdAt = new Date(issue!.createdAt);
    const firstCommentAt = new Date(issue!.comments!.nodes?.[0]!.createdAt);
    return firstCommentAt.getTime() - createdAt.getTime();
  });

  // Sort them based on response time
  issuesResponseTime.sort((a, b) => a - b);

  // Calculate the average
  const issuesTotalResponseTime = issuesResponseTime.reduce(
    (acc, responseTime) => acc + responseTime,
    0,
  );
  const issuesResponseAverageAge =
    issuesCount > 0 ? issuesTotalResponseTime / issuesCount : 0;

  // Calculate the median
  const issuesResponseMedianAge =
    issues.length > 0 ? issuesResponseTime[Math.floor(issues.length / 2)] : 0;

  return {
    issuesCount,
    issuesResponseAverageAge,
    issuesResponseMedianAge,
  };
};

// Number of repos to process in parallel. 10 concurrent × 3 calls = 30 in-flight
// requests at a time, which comfortably fits within GitHub's secondary rate limits.
const ISSUE_METRICS_CONCURRENCY = 10;

export const addIssueMetricsData: Fetcher = async (result, octokit, config) => {
  const repoNames = Object.keys(result.repositories);
  console.log(
    `📊  Fetching issue metrics for ${repoNames.length} repositories (concurrency: ${ISSUE_METRICS_CONCURRENCY})`,
  );

  const tasks = repoNames.map((repoName, index) => async () => {
    // Check rate limit headroom before each batch boundary (every CONCURRENCY repos)
    if (index % ISSUE_METRICS_CONCURRENCY === 0) {
      if (index > 0) {
        console.log(
          `📊  Progress: ${index}/${repoNames.length} repositories processed`,
        );
      }
      await throttleForRateLimit(octokit);
    }

    const {
      issuesAverageAge: openIssuesAverageAge,
      issuesMedianAge: openIssuesMedianAge,
    } = await calculateIssueMetricsPerRepo(repoName, 'open', octokit, config);

    const {
      issuesAverageAge: closedIssuesAverageAge,
      issuesMedianAge: closedIssuesMedianAge,
    } = await calculateIssueMetricsPerRepo(repoName, 'closed', octokit, config);

    const { issuesResponseAverageAge, issuesResponseMedianAge } =
      await calculateIssueResponseTime(repoName, octokit, config);

    return {
      repoName,
      openIssuesAverageAge,
      openIssuesMedianAge,
      closedIssuesAverageAge,
      closedIssuesMedianAge,
      issuesResponseAverageAge,
      issuesResponseMedianAge,
    };
  });

  const allMetrics = await runWithConcurrency(tasks, ISSUE_METRICS_CONCURRENCY);

  for (const metrics of allMetrics) {
    const repo = result.repositories[metrics.repoName];
    repo.openIssuesAverageAge = metrics.openIssuesAverageAge;
    repo.openIssuesMedianAge = metrics.openIssuesMedianAge;
    repo.closedIssuesAverageAge = metrics.closedIssuesAverageAge;
    repo.closedIssuesMedianAge = metrics.closedIssuesMedianAge;
    repo.issuesResponseAverageAge = metrics.issuesResponseAverageAge;
    repo.issuesResponseMedianAge = metrics.issuesResponseMedianAge;
  }

  return result;
};
