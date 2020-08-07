# -*- coding: UTF-8 -*-

import time
from sklearn.externals import joblib

from debt_risk_algorithm.auxiliary_function import *

#数据标准化
# def _data_forward_normalization(colname_values):
#     normal_data=MaxMinNormalization(colname_values)
#     return normal_data
#
# def _data_backward_normalization(colname_values):
#     normal_data=MinMaxNormalization(colname_values)
#     return normal_data


# 企业债务评分
def _conpany_risk_score(normal_data):
    fa_company = load_model('model_files/fa_company.pkl')
    fa_area.fit(normal_data)
    weight=fa_company.get_factor_variance()[1]
    factor_score =fa_company.transform(normal_data)
    score=(np.dot(factor_score,weight)/weight.sum()).real
    result=MaxMinNormalization(score)*100
    # result[result == 0] = 3
    # result[result == 1] = 1
    return result



# 企业债务评分数据结果返回函数
def __batch_area_result(config_params):
    cols = ['sum_FinanceMoney','sum_DebtBalance','sum_OperationalDebt_FinanceMoney','sum_ImplicitDebt_FinanceMoney',
'sum_OperationalDebt_DebtBalance','sum_ImplicitDebt_DebtBalance',
'sum_CreateAudit_FinanceMoney',
'sum_CreateAudit_DebtBalance',
'sum_StockDebt_FinanceMoney','sum_DirectFinance_FinanceMoney','sum_Others_FinanceMoney',
'sum_DirectFinance_DebtBalance','sum_Others_DebtBalance','sum_WithdrawMoney','CurrentAssets_YearEndBalance',
'CurrentAssets_YearStartBalance','NotCurrentAssets_YearEndBalance','NotCurrentAssets_YearStartBalance',
'AssetsTotal_YearEndBalance','AssetsTotal_YearStartBalance','LiabilitiesTotal_YearEndBalance','LiabilitiesTotal_YearStartBalance',
'TotalOwnersEquity_YearEndBalance',
'TotalLiabilitiesOwnersEquity_YearEndBalance','TotalLiabilitiesOwnersEquity_YearStartBalance',
'sum_AccountBalance_Normal','sum_AvailableBalance_Normal','Capital']

    forward = ['sum_FinanceMoney', 'sum_DebtBalance', 'sum_OperationalDebt_FinanceMoney', 'sum_ImplicitDebt_FinanceMoney',
         'sum_OperationalDebt_DebtBalance', 'sum_ImplicitDebt_DebtBalance',
         'sum_CreateAudit_FinanceMoney',
         'sum_CreateAudit_DebtBalance',
         'sum_StockDebt_FinanceMoney', 'sum_DirectFinance_FinanceMoney', 'sum_Others_FinanceMoney',
         'sum_DirectFinance_DebtBalance', 'sum_Others_DebtBalance', 'sum_WithdrawMoney',
         'LiabilitiesTotal_YearEndBalance', 'LiabilitiesTotal_YearStartBalance']
    backward = ['CurrentAssets_YearEndBalance',
         'CurrentAssets_YearStartBalance', 'NotCurrentAssets_YearEndBalance', 'NotCurrentAssets_YearStartBalance',
         'AssetsTotal_YearEndBalance', 'AssetsTotal_YearStartBalance',
         'TotalOwnersEquity_YearEndBalance',
         'TotalLiabilitiesOwnersEquity_YearEndBalance', 'TotalLiabilitiesOwnersEquity_YearStartBalance',
         'sum_AccountBalance_Normal', 'sum_AvailableBalance_Normal', 'Capital']

    trade_select_sql = '''sql = select CompanyId,sum_FinanceMoney,sum_DebtBalance,sum_OperationalDebt_FinanceMoney,sum_ImplicitDebt_FinanceMoney,
sum_OperationalDebt_DebtBalance,sum_ImplicitDebt_DebtBalance,
sum_CreateAudit_FinanceMoney,
sum_CreateAudit_DebtBalance,
sum_StockDebt_FinanceMoney,sum_DirectFinance_FinanceMoney,sum_Others_FinanceMoney,
sum_DirectFinance_DebtBalance,sum_Others_DebtBalance,sum_WithdrawMoney,CurrentAssets_YearEndBalance,
CurrentAssets_YearStartBalance,NotCurrentAssets_YearEndBalance,NotCurrentAssets_YearStartBalance,
AssetsTotal_YearEndBalance,AssetsTotal_YearStartBalance,LiabilitiesTotal_YearEndBalance,LiabilitiesTotal_YearStartBalance,
TotalOwnersEquity_YearEndBalance,
TotalLiabilitiesOwnersEquity_YearEndBalance,TotalLiabilitiesOwnersEquity_YearStartBalance,
sum_AccountBalance_Normal,sum_AvailableBalance_Normal,Capital
from  eb_Lgd_Debt_Base
'''


    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = trade_data_list[cols]

    # 数据标准化
    model_data_list[forward]= MaxMinNormalization(model_data_list[forward])
    model_data_list[backward]=MinMaxNormalization(model_data_list[backward])

    normal_data=model_data_list


    # 获取计算结果
    analysis_result_list = _company_risk_score(normal_data)

#以下代码未修改
    result_list = []
    for i in range(len(analysis_result_list)):
        result_list.append({
            'enterprise_original_id': trade_data_list['company_id'][i],
            'uscc': trade_data_list['company_uscc'][i],
            'name': trade_data_list['company_name'][i],
            'enterprise_nature': analysis_result_list[i]
        })

    return result_list



def __get_batch_insert_sql(table_name, batch_no, enterprise_nature_list):
    batch_insert_sql = "insert into %s(create_time, update_time, batch_no, enterprise_original_id, " \
                       "uscc, name, enterprise_nature) values " % (table_name or 'ar_enterprise_nature')
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for en in enterprise_nature_list:
        batch_insert_sql += "('%s', '%s', %s, '%s', '%s', '%s', %s)," \
                            % (current_time, current_time, batch_no,
                               en['enterprise_original_id'], en['uscc'],
                               en['name'], en['enterprise_nature'])
    return batch_insert_sql[:-1] + " returning id"


def enterprise_nature_analysis(config_params):
    enterprise_nature_list = __batch_company_result(config_params)
    if enterprise_nature_list is None:
        return
    delete_by_sql(config_params, "delete from %s where batch_no = %s"
                  % ((config_params['table'] or 'ar_enterprise_nature'), config_params['batch_no']))

    db_config = {
        'database': config_params['database'],
        'db_user': config_params['user'],
        'db_passwd': config_params['password'],
        'db_port': config_params['port'],
        'db_host': config_params['host']
    }
    db = DatabaseOperator(database_config_path=None, database_config=db_config)

    for data_list in list_partition(enterprise_nature_list, 100):
        db.pg_insert_operator(__get_batch_insert_sql(config_params['table'], config_params['batch_no'], data_list))


def main():
    pass


if __name__ == "__main__":
    main()
