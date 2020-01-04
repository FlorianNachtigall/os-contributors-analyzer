from github import Github
import src.analyzer as a

####################################

# get_prs_for_repo("cloudfoundry-incubator", "cf-openstack-validator")
# get_orgs_for_repo("cloudfoundry-incubator/cf-openstack-validator")

####################################

# volunteer_acceptance_rate, employee_acceptance_rate = a.calculate_pr_acceptance_rate("kubernetes", "kubernetes")
# # volunteer_acceptance_rate, employee_acceptance_rate = c.calculate_pr_acceptance_rate("cloudfoundry")
# print("volunteer_acceptance_rate is: " + str(volunteer_acceptance_rate))
# print("employee_acceptance_rate is: " + str(employee_acceptance_rate))

####################################

print(a.calculate_pr_acceptance_rate_by_companies("kubernetes", "kubernetes"))

####################################

# volunteer_avg_time, employee_avg_time = c.calculate_issue_processing_time("cloudfoundry")
# print("volunteer_avg_time is: " + str(volunteer_avg_time))
# print("employee_avg_time is: " + str(employee_avg_time))

####################################

# with open('github-token', 'r') as token_file:
#     token = token_file.read().rstrip("\n")
 
# g = Github(token)

# # for i in g.get_organization("cloudfoundry").get_issues(filter="all", state="closed"):
# #     print(i.pull_request)

# # for issue in g.get_organization("cloudfoundry").get_issues(filter="all"):
# #     print(issue.title)

# ####################################

# org = "kubernetes"
# repo = "kubectl"

# i = 0
# print("####### RATE LIMIT: " + str(g.get_rate_limit().core))
# for pull in g.get_repo(org + "/" + repo).get_pulls(state="closed"):     
#         i += 1
#         pull.
#         user = pull.user.login
#         print(pull.state)
#         print(user)

# # i = 0
# # print("####### RATE LIMIT: " + str(g.get_rate_limit().core))
# # for issue in g.get_repo(org + "/" + repo).get_issues(state="closed"):     
# #         i += 1
# #         user = issue.user.login
# #         print(issue.pull_request)
# #         print(user)

# print("####### RATE LIMIT: " + str(g.get_rate_limit().core))
# print("NUM of pulls: " + str(i))