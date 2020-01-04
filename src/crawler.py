from github import Github
from collections import Counter
import pandas as pd
import numpy as np
import json
import re

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")

g = Github(token)
pull_file_suffix = "pulls.csv"
user_file_suffix = "users.csv"
detailed_pull_file_suffix = "detailed_pulls.csv"

def crawl(org = "kubernetes", repo = "kubernetes"):
    crawl_pulls(org, repo)
    pulls = get_pulls(org, repo)

    crawl_pull_authors(pulls, org, repo)
    users = get_pull_authors(org, repo)

    merge_pulls_with_users(pulls, users, org, repo)
    get_companies(org, repo)

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

def get_pulls(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + pull_file_suffix, sep='\t', header=1, names=["number", "user_login", "created_at", "closed_at", "merged_at", "title"])

def crawl_pull_authors(pulls, org, repo):
    users = []
    for user_login_name in _get_top_user_logins(pulls, 5):
        print(user_login_name)
        user = g.get_user(user_login_name)
        user_orgs = []
        for user_org in user.get_orgs():
            user_orgs.append(user_org.login)
        user_dict = {
            "user_login": user_login_name, 
            "user_company": user.company, 
            "user_mail": _extract_mail_domain(user.email), 
            "user_orgs": ','.join(user_orgs)
            }
        users.append(user_dict)

    users_df = pd.DataFrame(users, columns=["user_login", "user_company", "user_mail", "user_orgs"])
    users_df.to_csv(org + "_" + repo + "_" + user_file_suffix, sep='\t')

def get_pull_authors(org, repo):
    return pd.read_csv(org + "_" + repo + "_" + user_file_suffix, sep='\t', header=1, names=["user_login", "user_company", "user_mail", "user_orgs"])

def merge_pulls_with_users(pulls, users, org, repo):
    merge = pd.merge(pulls, users, on='user_login')
    print(merge.iloc[1:100])
    merge.to_csv(org + "_" + repo + "_" + detailed_pull_file_suffix, sep='\t')

def get_companies(org, repo):
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

    pull_authors = get_pull_authors(org, repo)
    user_companies = pull_authors["user_company"].values
    user_mails = pull_authors["user_mail"].values
    cleaned_user_companies = set(x for x in user_companies if str(x) != 'nan')
    cleaned_user_mails = set(x for x in user_mails if str(x) != 'nan')

    for k, v in companies.items():
        regex = re.compile(v["regex_identifier"], re.IGNORECASE)
        companies[k]["companies"] = list(filter(regex.search, cleaned_user_companies))
        companies[k]["mail_addresses"] = list(filter(regex.search, cleaned_user_mails))
        companies[k]["merged_pulls"] = []
        companies[k]["closed_pulls"] = []

    with open('companies.json', 'w') as f:
        json.dump(companies, f, sort_keys=True, indent=4)

def compare_users_with_devstats_data(devstats_filename):
    with open(devstats_filename, 'r') as f:
        datastore = json.load(f)
        df = pd.DataFrame(datastore)
        devstats_users = df["login"].values

        users = _get_user_logins(get_pull_authors("kubernetes", "kubernetes"))
        users_count = users
        intersection = {user for user in users if user in devstats_users}
        print(intersection)
        print("# of users: " + str(len(users)))
        print("# of common users: " + str(len(intersection)))
        print("percentage of users covered by devstats: " + str(len(intersection) / len(users)))

def _extract_mail_domain(mail_address):
    if mail_address is None:
        return
    mail_domain = re.search("@[\w.]+", mail_address)
    if mail_domain is None:
        return
    else:
        return mail_domain.group()

def _get_user_logins(pulls):
    user_logins = pulls["user_login"].values
    # user_logins = np.delete(user_logins, 0)
    # user_logins = list(dict.fromkeys(user_logins))
    user_logins = list(set(user_logins))
    print(len(user_logins))
    return user_logins

def _get_top_user_logins(pulls, number_of_pulls):
    user_logins = pulls["user_login"].values
    counter = Counter(user_logins)
    top_user_logins = []
    for user in counter:
        if counter[user] >= number_of_pulls:
            top_user_logins.append(user)
    print(len(top_user_logins))
    return top_user_logins