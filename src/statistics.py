from collections import Counter
import statsmodels.formula.api as sm
import pandas as pd
import numpy as np
import src.crawler as c
import src.preprocesser as p
import src.analyzer as a
import statsmodels.api as stats
import distance
import difflib
from factor_analyzer import FactorAnalyzer

def factor_analysis(org, repo):
    # https://www.datacamp.com/community/tutorials/introduction-factor-analysis
    # https://www.theanalysisfactor.com/the-fundamental-difference-between-principal-component-analysis-and-factor-analysis/
    # https://factor-analyzer.readthedocs.io/en/latest/factor_analyzer.html#module-factor_analyzer.factor_analyzer
    # https://towardsdatascience.com/factor-analysis-101-31710b7cadff
    issues1 = c.get_issues_with_response_time(org, repo, False)
    issues2 = c.get_issues_with_processing_time(org, repo, False)
    issues = pd.merge(issues1, issues2[["number", "processing_time", "closed_at"]], how="left", on="number")
    issues = issues[["company", "processing_time", "response_time", "priority"]]
    
    issues.dropna(subset=["processing_time", "response_time", "priority", "company"], inplace=True)
    issues.company.replace({'Google': 5,  'RedHat': 4, 'Microsoft': 3, 'VMware': 2, 'Huawei': 2, 'ZTE': 1}, inplace=True)
    issues["priority"] = issues["priority"].astype(float)
    issues["company"] = issues["company"].astype(float)
    print(issues.info())
    
    fa = FactorAnalyzer(rotation='varimax', n_factors=3)
    print(fa.fit(issues))
    print(fa.loadings_)
    print(fa.get_factor_variance())

def calculate_similarity_between_issue_response_and_processing_time(org, repo, based_on_devstats_data, companies):
    issue_processing_time = c.get_issues_with_processing_time(org, repo, based_on_devstats_data)
    issue_processing_time = p.filter_pull_requests_from_issues(org, repo, issue_processing_time)
    issue_processing_time_by_company = issue_processing_time.loc[issue_processing_time["company"].isin(companies)].groupby(['company'])['processing_time']
    
    issue_response_time = c.get_issues_with_response_time(org, repo, based_on_devstats_data)
    issue_response_time = p.filter_pull_requests_from_issues(org, repo, issue_response_time)
    issue_response_time_by_company = issue_response_time.loc[issue_response_time["company"].isin(companies)].groupby(['company'])['response_time']
    
    ac_rate = a.calculate_pr_acceptance_rate_by_companies(org, repo, based_on_devstats_data)
    company_order_ac_rate = [k for k, v in sorted(ac_rate.items(), key=lambda item: item[1])]
    company_order_ac_rate.reverse()

    print_similarity_between_lists(company_order_ac_rate, list(issue_response_time_by_company.mean().sort_values().index))
    print_similarity_between_lists(company_order_ac_rate, list(issue_processing_time_by_company.mean().sort_values().index))
    print_similarity_between_lists(list(issue_processing_time_by_company.mean().sort_values().index), list(issue_response_time_by_company.mean().sort_values().index))
    print_similarity_between_lists(list(issue_processing_time_by_company.median().sort_values().index), list(issue_response_time_by_company.median().sort_values().index))

def print_similarity_between_lists(list_1, list_2):
    print(list_1)
    print(list_2)
    print(distance.levenshtein(list_1, list_2))
    print(distance.hamming(list_1, list_2))
    print(difflib.SequenceMatcher(None,list_1,list_2).ratio())

def print_descriptive_metrics(issues, attribute, companies):
    print("########### CALCULATING STATS FOR '{}' ###########".format(attribute))
    print("Overall median: " + str(issues[attribute].median()))
    print("Overall mean: " + str(issues[attribute].mean()))
    google_issues = issues.loc[issues["company"] == "Google"]
    non_google_issues = issues.loc[issues["company"] != "Google"]
    issues = issues.loc[issues["company"].isin(companies)]
    print("Google vs. Rest median ratio: " + str(non_google_issues[attribute].median() / google_issues[attribute].median()))
    print("Google vs. Rest mean ratio: " + str(non_google_issues[attribute].mean() / google_issues[attribute].mean()))

    stats = issues.groupby(['company'])[attribute].describe()
    stats["median"] = issues.groupby(['company'])[attribute].median()
    stats["var"] = issues.groupby(['company'])[attribute].var()
    print("Median range between companies: " + str(stats["median"].max() - stats["median"].min()))
    print("Max median ratio between companies: " + str(stats["median"].max() / stats["median"].min()))
    print("Mean range between companies: " + str(stats["mean"].max() - stats["mean"].min()))
    print("Max mean ratio between companies: " + str(stats["mean"].max() / stats["mean"].min()))
    print("Variance ratio between companies: " + str(stats["var"].max() / stats["var"].min()))
    print("Avg difference between mean and median: " + str(issues[attribute].mean() / issues[attribute].median()))
    stats = stats.reindex(['count', 'mean', 'median', 'std', 'var', 'min', '25%', '50%', '75%', 'max'], axis=1)
    # stats = stats.loc[_get_general_company_order()]
    stats = stats.sort_values(by=["mean"])
    stats = np.round(stats, decimals=0)
    # stats = stats.apply(pd.to_numeric, downcast='integer')
    print(stats)
    stats.to_csv(attribute + "_stats.csv", sep=',')

def print_logistic_regression_for_pr_acceptance_rate(org, repo, based_on_devstats_data=False):
    users = p.get_users(org, repo, based_on_devstats_data)
    issues = c.get_issues(org, repo)
    pulls = c.get_pulls(org, repo)
    pulls = p.merge_pulls_with_issue_priority_and_kind(pulls, issues)
    pulls = _merge_pulls_with_company_column(pulls, users, based_on_devstats_data)
    pulls = p.add_dummy_column_for_pr_merge_state(pulls)
    pulls = _add_controlling_variables(pulls)
    pulls = _prepare_independent_company_variable(pulls, based_on_devstats_data)

    result = sm.logit(formula=_ols_formula("pr_is_merged", based_on_devstats_data), data=pulls).fit()
    _print_company_representation_in_data(pulls)
    _print_and_save_result(result)
    # ToDo: Add Chi-square Test https://pythonfordatascience.org/chi-square-test-of-independence-python/

def print_ols_regression_for_issue_response_time(org, repo, based_on_devstats_data=False):
    issues = c.get_issues_with_response_time(org, repo, based_on_devstats_data)
    issues = p.filter_pull_requests_from_issues(org, repo, issues)
    issues = _add_controlling_variables(issues)
    issues = _add_devstats_controlling_variables(issues) if based_on_devstats_data else issues
    issues = _prepare_independent_company_variable(issues, based_on_devstats_data)

    result = sm.ols(formula=_ols_formula("response_time", based_on_devstats_data), data=issues).fit().get_robustcov_results("HC1")
    _print_company_representation_in_data(issues)
    _print_and_save_result(result)
    _print_anova_results(issues, "response_time")

def print_ols_regression_for_issue_processing_time(org, repo, based_on_devstats_data=False):
    issues = c.get_issues_with_processing_time(org, repo, based_on_devstats_data)
    issues = p.filter_pull_requests_from_issues(org, repo, issues)
    issues = _add_controlling_variables(issues)
    issues = _add_devstats_controlling_variables(issues) if based_on_devstats_data else issues
    issues = _prepare_independent_company_variable(issues, based_on_devstats_data)
 
    result = sm.ols(formula=_ols_formula("processing_time", based_on_devstats_data), data=issues).fit().get_robustcov_results("HC1")
    _print_company_representation_in_data(issues)
    _print_and_save_result(result)
    _print_anova_results(issues, "processing_time")

def _print_anova_results(issues, dependent_variable):
    result = sm.ols(formula="{} ~ C(company)".format(dependent_variable), data=issues).fit()
    print(stats.stats.anova_lm(result, typ=2, robust="hc1") )
    print("R Squared for Anova: " + str(result.rsquared))

def print_ols_regression_for_issue_prioritization(org, repo):
    print("!WARNING! - Not suitable for contributor treatment analysis because labels are often assigned by contribtuor herself*himself.")
    issues = c.get_issues_with_company(org, repo)
    issues = issues.dropna(subset=["kind", "priority"]) 
    issues = issues.loc[issues["priority"] != 5]
    issues = _add_controlling_variables(issues)
    # issues = issues.loc[issues["company"] != "Huawei"] # due to too low number of issues

    # need to define reference category for kind
    result = sm.ols(formula="priority ~ C(company, Treatment(reference='Google')) + user_contributions + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + deprecation + bug", data=issues).fit()
    _print_company_representation_in_data(issues)
    _print_and_save_result(result)
    
def _ols_formula(dependent_variable, based_on_devstats_data=False):
    if based_on_devstats_data:
        # kind 'deprecation' is missing from logistic regression because no issues have that label -> print(Counter(pulls.deprecation.values))
        # return "{} ~ C(company, Treatment(reference='Google')) + C(priority, Treatment(reference='not_set')) + C(year, Treatment(reference=2014)) + C(country_id, Treatment(reference='us')) + C(tz, Treatment(reference='America')) + user_contributions + age + C(sex, Treatment(reference='m')) + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)
        # return "{} ~ C(company, Treatment(reference='Google')) + C(priority, Treatment(reference='not_set')) + C(country_id, Treatment(reference='us')) + user_contributions + age + C(sex, Treatment(reference='m')) + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)
        # return "{} ~ C(company, Treatment(reference='Google')) + C(priority, Treatment(reference='not_set')) + C(year, Treatment(reference=2014)) + C(country_id, Treatment(reference='us')) + user_contributions + age + C(sex, Treatment(reference='m')) + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)
        return "{} ~ C(company, Treatment(reference='group_1(google)')) + C(priority, Treatment(reference='not_set')) + C(year, Treatment(reference=2014)) + C(country_id, Treatment(reference='us')) + user_contributions + age + C(sex, Treatment(reference='m')) + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)
    else:
        return "{} ~ C(company, Treatment(reference='Google')) + user_contributions + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)
        # return "{} ~ C(company, Treatment(reference='Google')) + C(priority, Treatment(reference=2)) + user_contributions + Q('failing-test') + feature + cleanup + documentation + flake + Q('api-change') + design + bug".format(dependent_variable)

def _print_company_representation_in_data(df):
    print(pd.DataFrame.from_dict(Counter(df.company.values), orient='index').sort_values([0]))

def _add_controlling_variables(issues):
    issues = p.add_dummy_column_for_each_kind(issues)
    issues = p.add_dummy_column_for_rounded_year(issues)
    issues = p.add_column_for_user_contribution_strength(issues)
    issues = _preprocess_missing_priorities(issues)
    return issues

def _preprocess_missing_priorities(issues):
    issues = issues.dropna(subset=["priority"])
    print(Counter(issues.priority.values))
    print(issues.loc[issues["priority"] != 5].loc[issues["company"] == "Microsoft"].priority.mean())
    print(issues.loc[issues["priority"] != 5].loc[issues["company"] == "Google"].priority.mean())
    issues.priority.replace(to_replace=[4,5], value="not_set", inplace=True)
    # mean = issues.loc[issues["priority"] != 5].priority.mean()
    # issues.priority.replace({5: mean}, inplace=True)
    return issues

def _prepare_independent_company_variable(issues, based_on_devstats_data):
    if based_on_devstats_data:
        # issues = _reduce_companies_when_using_devstats_data(issues)
        issues = _group_companies_when_using_devstats_data(issues)
        # issues.company.replace({"(Unknown)": "unknown", "Independent": "unknown", None: "unknown"}, inplace=True)
    issues["company"].fillna("unknown", inplace=True) # replaces nan and None values
    return issues

def _reduce_companies_when_using_devstats_data(issues):
    companies = ['Google', 'Huawei', 'Microsoft', 'RedHat', 'VMware', 'ZTE', 'Fujitsu', 'IBM']
    # companies = [company for company, count in Counter(issues.company.values).most_common(15)]
    # companies = list(set(companies) - {"(Unknown)", "Independent", "NotFound", "?"}) # devstats uses different keywords for contributors without company affiliation, we don't differentiate and want to group them
    issues['company'] = np.where(issues['company'].isin(companies), issues['company'], 'unknown')
    return issues

def _group_companies_when_using_devstats_data(issues):
    group_1 = ['Google']
    group_2 = ['Huawei', 'Microsoft', 'RedHat', 'VMware']
    group_3 = ['ZTE', 'Fujitsu', 'IBM']
    issues['company'].replace(to_replace=group_1, value="group_1(google)", inplace=True)
    issues['company'].replace(to_replace=group_2, value="group_2", inplace=True)
    issues['company'].replace(to_replace=group_3, value="group_3", inplace=True)
    issues['company'] = np.where(issues['company'].isin(["group_1(google)", "group_2", "group_3"]), issues['company'], 'unknown')
    print(issues)
    return issues

def _add_devstats_controlling_variables(issues):
    devstats_user = p.get_formatted_devstats_user()
    issues = pd.merge(issues, devstats_user[["user_login", "commits", "age", "sex", "country_id", "tz"]], how="left", on="user_login")
    issues["sex"].fillna("unknown", inplace=True)
    issues["country_id"].fillna("unknown", inplace=True)
    issues["tz"].fillna("unknown", inplace=True)
    issues["tz"] = issues["tz"].apply(lambda tz: tz.split('/')[0])
    issues = _omit_single_value_occurences_in_df_to_prevent_singular_matrix_error(issues, "tz")
    # issues["country_id"].replace(to_replace="ni", value="ch", inplace=True)
    print(Counter(issues.loc[issues["company"] == "Microsoft"]["country_id"].values))

    print(Counter(issues.tz.values))
    print(Counter(issues.sex.values))
    issues = _reduce_countries_when_using_devstats_data(issues)
    print(Counter(issues["country_id"].values))
    return issues

def _omit_single_value_occurences_in_df_to_prevent_singular_matrix_error(issues, attribute):
    counter = Counter(issues[attribute].values)
    single_occurences = [x for x in counter if counter[x] <= 700]
    issues[attribute].replace(to_replace=single_occurences, value="others", inplace=True)
    return issues

def _reduce_countries_when_using_devstats_data(issues):
    other_countries = list(set(issues["country_id"].values) - {'us', 'cn', 'pl', 'de', 'in'})
    issues["country_id"].replace(to_replace=other_countries, value="others", inplace=True)
    return issues


def _merge_pulls_with_company_column(pulls, user, based_on_devstats_data):
    if based_on_devstats_data:
        pulls = pd.merge(pulls, user[["user_login", "company", "commits", "age", "sex", "country_id"]], how="left", on="user_login")
        pulls["sex"].fillna("unknown", inplace=True)
        print(Counter(pulls.sex.values))
        pulls["country_id"].fillna("unknown", inplace=True)
        # pulls["country_id"].replace(to_replace="ni", value="ch", inplace=True)
        pulls = _reduce_countries_when_using_devstats_data(pulls)
        print(Counter(pulls["country_id"].values))

    else:
        pulls = p.merge_issues_with_company_column(pulls, user)
    return pulls

def _print_and_save_result(result):
    print(result.summary())
    with open('final_ols_regression_results.txt','a') as f:
        f.write(result.summary().as_text())
    with open('final_ols_regression_results.csv','a') as f:
        f.write(result.summary().as_csv())

def _get_general_company_order():
    return ['Google', 'Microsoft', 'RedHat', 'VMware', 'Huawei', 'Fujitsu', 'IBM', 'ZTE']