# -*- coding: UTF-8 -*-

import time
from sklearn.externals import joblib

from debt_risk_algorithm.auxiliary_function import *

#数据标准化
def _data_normalization(colname_values):
    normal_data=MaxMinNormalization(colname_values)
    return normal_data




# 企业债务评分
def _area_risk_score(normal_data):
    fa_area = load_model('model_files/fa_area.pkl')
    fa_area.fit(normal_data)
    weight=fa_area.get_factor_variance()[1]
    factor_score =fa_area.transform(normal_data)
    score=(np.dot(factor_score,weight)/weight.sum()).real
    result=MaxMinNormalization(score)*100
    # result[result == 0] = 3
    # result[result == 1] = 1
    return result



# 企业债务评分数据结果返回函数
def __batch_area_result(config_params):
    cols = ['GovernmentSexDebtRatio', 'GovernmentDebtRatio', 'ImplicitDebtRatio', 'AllDebtServiceRatio',
            'ImplicitDebtServiceRatio', 'GovernmentDebtServiceRatio',
            'ConcernedDebtServiceRatio', 'OperationalDebtServiceRatio', 'AllInterestExpenseRatio',
            'ImplicitDebtInterestExpenseRatio', 'GovernmentDebtInterestExpenseRatio',
            'ConcernedDebtInterestExpenseRatio', 'OperationalDebtInterestExpenseRatio',
            'ImplicitDebtGetNewToOldRatio', 'AllStatisticsDebtBalance', 'GovernmentStatisticsDebtBalance',
            'ImplicitStatisticsDebtBalance',
            'ConcernedStatisticsDebtBalance', 'OperationalStatisticsDebtBalance',
            'ImplicitExcludeStatisticsDebtBalance']

    trade_select_sql = '''select AreaId,AreaName,LevelType,Year,GovernmentSexDebtRatio,GovernmentDebtRatio,ImplicitDebtRatio,AllDebtServiceRatio,
    ImplicitDebtServiceRatio,GovernmentDebtServiceRatio,ConcernedDebtServiceRatio,OperationalDebtServiceRatio,AllInterestExpenseRatio,
    ImplicitDebtInterestExpenseRatio,GovernmentDebtInterestExpenseRatio,ConcernedDebtInterestExpenseRatio,OperationalDebtInterestExpenseRatio,
    ImplicitDebtGetNewToOldRatio,AllStatisticsDebtBalance,GovernmentStatisticsDebtBalance,ImplicitStatisticsDebtBalance,
    ConcernedStatisticsDebtBalance,OperationalStatisticsDebtBalance,ImplicitExcludeStatisticsDebtBalance
from  ds_area_debt_risk_base_info

'''
    # 获取交易数据
    trade_data_list = panda_read_sql(config_params, trade_select_sql)
    # 获取模型计算所需数据
    model_data_list = trade_data_list[cols]

    # 数据标准化
    normal_data= _data_normalization(model_data_list)

    # 获取计算结果
    analysis_result_list = _area_risk_score(normal_data)

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
