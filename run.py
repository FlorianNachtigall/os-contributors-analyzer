from github import Github
import src.crawler as c

####################################

# get_prs_for_repo("cloudfoundry-incubator", "cf-openstack-validator")
# get_orgs_for_repo("cloudfoundry-incubator/cf-openstack-validator")

####################################

volunteer_acceptance_rate, employee_acceptance_rate = c.calculate_pr_acceptance_rate("cloudfoundry", "bosh-aws-cpi-release")
# volunteer_acceptance_rate, employee_acceptance_rate = c.calculate_pr_acceptance_rate("cloudfoundry")
print("volunteer_acceptance_rate is: " + str(volunteer_acceptance_rate))
print("employee_acceptance_rate is: " + str(employee_acceptance_rate))

####################################

volunteer_avg_time, employee_avg_time = c.calculate_issue_processing_time("cloudfoundry")
print("volunteer_avg_time is: " + str(volunteer_avg_time))
print("employee_avg_time is: " + str(employee_avg_time))

####################################

# with open('github-token', 'r') as token_file:
#     token = token_file.read().rstrip("\n")
 
# g = Github(token)

# for i in g.get_organization("cloudfoundry").get_issues(filter="all", state="closed"):
#     print(i.pull_request)

# for issue in g.get_organization("cloudfoundry").get_issues(filter="all"):
#     print(issue.title)