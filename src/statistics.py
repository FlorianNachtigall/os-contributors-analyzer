import statsmodels.api as sm
import pandas as pd
from collections import Counter
from statsmodels.formula.api import ols
import src.crawler as c
import src.preprocesser as p


def print_ols_test_for_pr_acceptance_rate(org, repo):
    users = c.get_issue_authors_with_company(org, repo)
    pulls = c.get_pulls(org, repo)
    pulls = p.merge_issues_with_company_column(pulls, users)
    pulls = p.add_dummy_column_for_pr_merge_state(pulls)
    result = ols(formula="merged ~ company", data=pulls).fit()
    print(result.params)
    print(result.summary())

def print_ols_test_for_issue_processing_time(org, repo):
    issues = c.get_issues_with_processing_time(org, repo)
    issues = issues.loc[issues["priority"] != 5]
    # issues = issues.dropna(subset=["kind"]) 
    issues = p.add_dummy_column_for_each_kind(issues)
    issues = p.add_column_for_user_contribution_strength(issues)
    print(issues.sort_values(by=["user_contributions"]))
    result = ols(formula="processing_time ~ company", data=issues).fit()
    print(result.params)

    # result = ols(formula="processing_time ~ C(company)", data=issues).fit()
    # print(sm.stats.anova_lm(result, typ=2))

def print_ols_test_for_issue_prioritization(org, repo):
    issues = c.get_issues_with_company(org, repo)
    issues = issues.dropna(subset=["kind", "priority"]) 
    issues["company"].fillna('unknown', inplace=True)
    issues = issues.loc[issues["priority"] != 5]
    # issues = issues.loc[issues["company"] != "Huawei"]
    issues = p.add_dummy_column_for_each_kind(issues)
    issues = p.add_column_for_user_contribution_strength(issues)
    # dummies = pd.get_dummies(issues.company)
    # issues = issues.join(dummies)
    # print(issues.sort_values(by=["user_contributions"]))
    print(pd.DataFrame.from_dict(Counter(issues.company.values), orient='index'))
    result = ols(formula="priority ~ C(company, Treatment(reference='unknown')) + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + deprecation + bug + user_contributions", data=issues).fit()
    # result = ols(formula="priority ~ cleanup + documentation + flake + Q('api-change') + design + deprecation + bug + user_contributions", data=issues).fit()
    # print(result.params)
    print(result.summary())