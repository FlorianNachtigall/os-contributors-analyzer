from github import Github
from github import GithubException
from collections import Counter
from datetime import datetime
from datetime import timedelta
import pandas as pd
import os.path
import time
import json
import re

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")

g = Github(token)

company_file_suffix = "companies.json"
pull_file_suffix = "pulls.csv"
issue_file_suffix = "issues.csv"
issue_comments_file_suffix = "issues_comments.csv"
user_file_suffix = "users.csv"

def crawl(org, repo):
    crawl_pulls(org, repo)
    crawl_issues_with_comments(org, repo)
    issues = get_issues_with_comments(org, repo)
    crawl_issue_authors(issues, org, repo)
    determine_companies(org, repo)

def crawl_pulls(org, repo):
    repo_object = g.get_repo(org + "/" + repo)
    pulls = []
    for pull in repo_object.get_pulls(state="closed"):      
        pull_dict = {
            "number": pull.number,
            "user_login": pull.user.login,
            "created_at": pull.created_at,
            "closed_at": pull.closed_at,
            "merged_at": pull.merged_at,
            "title": pull.title
            }
        pulls.append(pull_dict)
    pulls_df = pd.DataFrame(pulls, columns=["number", "user_login", "created_at", "closed_at", "merged_at", "title"])
    pulls_df.to_csv(org + "_" + repo + "_" + pull_file_suffix, sep='\t')

def crawl_issues(org, repo):
    repo_object = g.get_repo(org + "/" + repo)
    issues = []
    i = 0
    for issue in repo_object.get_issues(state="closed"):
        issue_dict = {
            "number": issue.number,
            "user_login": issue.user.login,
            "created_at": issue.created_at,
            "closed_at": issue.closed_at,
            "title": issue.title,
            "priority": _determine_priority(issue.labels),
            "kind": _determine_kind(issue.labels)
            }
        issues.append(issue_dict)
        i = i + 1
        if i % 10000 == 0:
            issues_df = pd.DataFrame(issues, columns=["number", "user_login", "created_at", "closed_at", "title", "priority", "kind"])
            issues_df.to_csv("small_batch_" + str(i) + org + "_" + repo + "_" + issue_file_suffix, sep='\t')

    issues_df = pd.DataFrame(issues, columns=["number", "user_login", "created_at", "closed_at", "title", "priority", "kind"])
    issues_df.to_csv(org + "_" + repo + "_" + issue_file_suffix, sep='\t')

def crawl_issues_with_comments(org, repo):
    buffer_size = 50
        
    while True:
        issues = []
        since = _get_time_of_last_issue(org, repo)
        repo_object = g.get_repo(org + "/" + repo)

        try:
            for issue in repo_object.get_issues(state="closed", sort="updated", since=since, direction="asc"):
                comment = _get_first_comment(issue)
                if not comment:
                    continue
                issue_dict = {
                    "number": issue.number,
                    "user_login": issue.user.login,
                    "commentator": comment.user.login,
                    "author_association": comment.raw_data["author_association"],
                    "created_at": issue.created_at,
                    "commented_at": comment.created_at,
                    "updated_at": issue.updated_at,
                    "closed_at": issue.closed_at,
                    "title": issue.title,
                    "comment": comment.body,
                    "priority": _determine_priority(issue.labels),
                    "kind": _determine_kind(issue.labels)
                    }
                issues.append(issue_dict)

                if len(issues) % buffer_size == 0:
                    issues_df = pd.DataFrame(issues, columns=list(issue_dict.keys()))
                    issues_df.to_csv(org + "_" + repo + "_" + issue_file_suffix + "with_comments", sep='\t', index=False, mode='a', header=False)
                    issues = []
                _respectRateLimit()

        except Exception as e:
            print("### Getting issues with comments failed with: " + str(e))
            continue

        issues_df = pd.DataFrame(issues, columns=list(issue_dict.keys()))
        issues_df.to_csv(org + "_" + repo + "_" + issue_file_suffix + "with_comments", sep='\t', index=False, mode='a', header=False)
        break
        
def crawl_issue_comments(org, repo):
    time_format = "%Y-%m-%d %H:%M:%S"
    issue_comments = []
    i = 0
    raw_time = get_issue_comments(org, repo)["created_at"].iloc[-1]
    time = datetime.strptime(raw_time, time_format)
    print(time)

    repo_object = g.get_repo(org + "/" + repo)
    for comment in repo_object.get_issues_comments(since=time):
        comment_dict = {
            "issue": _determine_issue_number(comment.issue_url),
            "user_login": comment.user.login,
            "created_at": comment.created_at,
            "author_association": comment.raw_data["author_association"],
            "comment": comment.body
        }
        print(comment_dict["created_at"])
        issue_comments.append(comment_dict)
        i = i + 1
        if i % 1000 == 0:
            issue_comments_df = pd.DataFrame(issue_comments, columns=["issue", "user_login", "created_at", "author_association", "comment"])
            issue_comments_df.to_csv("small_" + str(i) + "_" + org + "_" + repo + "_desc_" + issue_comments_file_suffix, sep='\t')
        _respectRateLimit()

    issue_comments_df = pd.DataFrame(issue_comments, columns=["issue", "user_login", "created_at", "author_association", "comment"])
    issue_comments_df.to_csv(org + "_" + repo + "_" + issue_comments_file_suffix, sep='\t')

def crawl_issue_authors(issues, org, repo):
    batch_size = 100
    user_login_file = "users_left.csv"
    user_logins = _get_user_logins(issues)
    _ensure_user_file_exists(user_login_file, user_logins)
    
    while True:
        with open(user_login_file, 'r') as filehandle:
            user_logins = json.load(filehandle)
        if not user_logins:
            break
        
        users = crawl_users(user_logins[:batch_size], org, repo)
        users_df = pd.DataFrame(users, columns=["user_login", "user_company", "user_mail", "user_orgs"])
        users_df.to_csv(org + "_" + repo + "_" + user_file_suffix + "all_cache", sep='\t', index=False, mode='a', header=False)

        with open(user_login_file, 'w') as filehandle:
            json.dump(user_logins[batch_size:], filehandle)

def crawl_users(user_logins, org, repo):
    users = []
    for user_login_name in user_logins:
        print(user_login_name)
        _respectRateLimit()
        try:
            user = g.get_user(user_login_name)
        except GithubException:
            with open('users_not_found.log', 'a') as f:
                f.write(user_login_name)
            continue
        except:
            with open('failing_users.log', 'a') as f:
                f.write(user_login_name)
            continue
            
        user_orgs = []
        for user_org in user.get_orgs():
            user_orgs.append(user_org.login)
        user_dict = {
            "user_login": user_login_name, 
            "user_company": user.company, 
            "user_mail": extract_mail_domain(user.email), 
            "user_orgs": ','.join(user_orgs)
            }
        users.append(user_dict)
    return users

def get_issues_with_processing_time(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_file_suffix + "_with_processing_time_5", sep='\t', header=1, names=["number", "user_login", "company", "created_at", "closed_at", "processing_time", "title", "priority", "kind"])

def get_issues_with_response_time(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_file_suffix + "_with_response_time_3", sep='\t', header=1, names=["number", "user_login", "company", "created_at", "commented_at", "response_time", "title", "priority", "kind"])

def get_issue_comments(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_comments_file_suffix, sep='\t', header=0, names=["issue", "user_login", "created_at", "author_association", "comment"])

def get_issues_with_comments(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_file_suffix + "with_comments", sep='\t', header=None, names=["number", "user_login", "commentator", "author_association", "created_at", "commented_at", "updated_at", "closed_at", "title", "comment", "priority", "kind"])

def get_issues(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_file_suffix, sep='\t', header=1, names=["number", "user_login", "created_at", "closed_at", "title", "priority", "kind"])

def get_issues_with_company(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + issue_file_suffix + "with_employer", sep='\t', header=1, names=["number", "user_login", "created_at", "closed_at", "title", "priority", "kind", "company"])

def get_pulls(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + pull_file_suffix, sep='\t', header=1, names=["number", "user_login", "created_at", "closed_at", "merged_at", "title"])

def get_users(org, repo):
    return get_issue_authors(org, repo) # concating not needed as PRs are subset of issues pd.concat([get_pull_authors(org, repo), get_issue_authors(org, repo)]).drop_duplicates().reset_index(drop=True)
        
def get_pull_authors(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + user_file_suffix + "all_cache", sep='\t', header=None, names=["user_login", "user_company", "user_mail", "user_orgs"])

def get_pull_authors_with_company(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + user_file_suffix + "_with_company", sep='\t', header=1, names=["user_login", "user_company", "user_mail", "user_orgs", "company"])

def get_issue_authors(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + user_file_suffix + "issues_all_cache", sep='\t', header=None, names=["user_login", "user_company", "user_mail", "user_orgs"])

def get_issue_authors_with_company(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + user_file_suffix + "_issues_with_company", sep='\t', header=1, names=["user_login", "user_company", "user_mail", "user_orgs", "company"])

def get_companies(org, repo):
    with open(org + "_" + repo + "_" + company_file_suffix, 'r') as f:
        return json.load(f)
            
def extract_mail_domain(mail_address):
    if mail_address is None:
        return
    mail_domain = re.search("@[\w.]+", mail_address)
    if mail_domain is None:
        return
    else:
        return mail_domain.group()

def get_repos_for_org(org):
    repos = []
    for repo in g.get_organization("cloudfoundry").get_repos(type="public"):
        repos.append(repo.name)
    return repos

def get_orgs_for_repo(repo):
    mail_domains = []
    orgs = []
    companys = []

    for contributor in g.get_repo(repo).get_contributors():
        for org in contributor.get_orgs():
            orgs.append(org.login)
        companys.append(contributor.company)
        mail = contributor.email or ""
        mail_domain = re.search("@[\w.]+", mail)
        if mail_domain is not None:
            mail_domains.append(mail_domain.group())
        
    print(mail_domains)
    print(orgs)
    print(companys)

    print(Counter(mail_domains))
    print(Counter(orgs))
    print(Counter(companys))

def get_issues_for_org(org):
    return g.get_organization(org).get_issues(filter="all", state="closed")

def determine_companies(org, repo):
    companies = {
        "Google": {
            "regex_identifier": "google",
        },
        "RedHat": {
            "regex_identifier": "red\s?hat",
        },
        "Huawei": {
            "regex_identifier": "huawei",
        },
        "ZTE": {
            "regex_identifier": "zte",
        },
        "Microsoft": {
            "regex_identifier": "microsoft",
        },
        "VMware": {
            "regex_identifier": "vmware",
        }
    }

    issue_authors = get_issue_authors(org, repo)
    user_companies = issue_authors["user_company"].values
    user_mails = issue_authors["user_mail"].values
    cleaned_user_companies = set(x for x in user_companies if str(x) != 'nan')
    cleaned_user_mails = set(x for x in user_mails if str(x) != 'nan')

    for k, v in companies.items():
        regex = re.compile(v["regex_identifier"], re.IGNORECASE)
        companies[k]["companies"] = list(filter(regex.search, cleaned_user_companies))
        companies[k]["mail_addresses"] = list(filter(regex.search, cleaned_user_mails))
        companies[k]["merged_pulls"] = []
        companies[k]["closed_pulls"] = []

    with open(org + "_" + repo + "_" + company_file_suffix, 'w') as f:
        json.dump(companies, f, sort_keys=True, indent=4)

def _ensure_user_file_exists(user_login_file, user_logins):
    if not os.path.isfile(user_login_file):
        with open(user_login_file, 'w') as filehandle:
            json.dump(user_logins, filehandle)

def _respectRateLimit():
    while g.get_rate_limit().raw_data["core"]["remaining"] < 100:
            time.sleep(60)

def _get_user_logins(df):
    return list(set(df["user_login"].values))

def _get_top_user_logins(pulls, number_of_pulls):
    user_logins = pulls["user_login"].values
    counter = Counter(user_logins)
    top_user_logins = []
    for user in counter:
        if counter[user] >= number_of_pulls:
            top_user_logins.append(user)
    print(len(top_user_logins))
    return top_user_logins

def _get_first_comment(issue):
    bot_list = ["goodluckbot, k8s-cherrypick-bot", "athenabot", "k8s-github-robot", "googlebot", "k8s-reviewable", "k8s-ci-robot", "k8s-bot", "fejta-bot" "kubernetes-bot", "miabbot", "timbot"]
    for attempt in range(10):
        try:
            comments = issue.get_comments()
            for comment in comments:
                if comment.user.login != issue.user.login and comment.user.login not in bot_list:
                    if (comment.created_at - issue.created_at) > timedelta(seconds=30):
                        return comment
                    
                    with open('potential_bots.log', 'a') as f:
                        f.write(str(comment.user.login) + " : " + str(comment.created_at - issue.created_at) + "\n")
            return None
        except Exception as e:
            print("### Getting comments for issue " + str(issue) + "failed with: " + str(e))

def _get_time_of_last_issue(org, repo):
    time_format = "%Y-%m-%d %H:%M:%S"
    column = get_issues_with_comments(org, repo)["updated_at"]
    if len(column) > 0:
        return datetime.strptime(column.iloc[-1], time_format) # + timedelta(seconds=1) to avoid duplicates - alternativly do df.drop_duplicates().reset_index(drop=True)
    else:
        return datetime.fromtimestamp(1483228800)

def _determine_issue_number(url):
    match = re.search("issues\/(\d+)", url)
    if match is not None:
        return match.group(1)

def _determine_kind(labels):
    kinds = []
    for label in labels:
        kind = re.search("kind\/(.+)", label.name)
        if kind is not None:
            kinds.append(kind.group(1))
    return ",".join(kinds) # not-categorized

def _determine_priority(labels):
    priority_mapping = {
        "critical-urgent": 0,
        "important-soon": 1,
        "important-longterm": 2,
        "backlog": 3,
        "awaiting-more-evidence": 4,
        "not-prioritized": 5,
        "P0": 0,
        "P1": 1,
        "P2": 2,
        "P3": 3
    }
    for label in labels:
        prio = re.search("priority\/(.+)", label.name)
        if prio is not None:
            return priority_mapping[prio.group(1)]
    return 5 # not-prioritized