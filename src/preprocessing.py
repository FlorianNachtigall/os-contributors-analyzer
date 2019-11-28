from github import Github
from collections import Counter
import re

with open('github-token', 'r') as token_file:
    token = token_file.read().rstrip("\n")
 
g = Github(token)

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