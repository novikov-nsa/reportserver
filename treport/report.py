import configparser

report_config = {'schl_foms_89n_only':{
                                        'name': 'Сводный чек-лист (89н)',
                                        'template': '/Users/sergejnovikov/PycharmProjects/reportserver/treport_files/templates/schl_foms_89n_only.xlsx',
                                        'params': {'p_start_date': {'name':'Дата начала отчетного периода',
                                                                    'type': 'date',
                                                                    'required': True
                                                                    },
                                                   'p_end_date': {'name': 'Дата окончания отчетного периода',
                                                                  'type': 'date',
                                                                  'required': True},
                                                   'is_ks':{'name': 'Только КС',
                                                            'type': 'boolean',
                                                            'default_value': True,
                                                            'required': True},
                                                   'is_app': {'name': 'Только АПП',
                                                              'type': 'boolean',
                                                              'default_value': True,
                                                              'required': True},
                                                   'list_tfoms_codes': {
                                                       'name': 'Список кодов ТФОМС',
                                                       'type': 'sqlstring',
                                                       'required': False
                                                   }},
                                        'report_pages':{
                                                    'page1': {
                                                        'page_name': 'Р_1 Общая информация',
                                                        'sql_file': '/Users/sergejnovikov/PycharmProjects/reportserver/treport_files/sql/schl_foms_89n_only_page1.sql',
                                                        'header_rows': 13,
                                                        'ignored_columns': [26, 29, 32]
                                                    },
                                                    'page2': {
                                                        'page_name': 'Р_2 Сведения о ЗЛ и лечении КС',
                                                        'sql_file': '/Users/sergejnovikov/PycharmProjects/reportserver/treport_files/sql/schl_foms_89n_only_page2.sql',
                                                        'header_rows': 12,
                                                        'ignored_columns': [5, 6, 7, 15, 17, 21, 30, 33, 36, 39, 42, 45, 48, 51, 53, 55, 57, 59, 61]
                                                    }


                                        },
                                        'out_dir': '/Users/sergejnovikov/PycharmProjects/reportserver/treport_files/out/',
                                        'file_name_generate_rule': 'Сводный_чек-лист_{p_start_date}_{p_end_date}_'
                }
}

def get_config(path_to_inifile):
    config = configparser.ConfigParser()
    config.read(path_to_inifile)
    login = config.get('database', 'login')
    password = config.get('database', 'password')
    host = config.get('database', 'host')
    port = config.get('database', 'port')
    database = config.get('database', 'database')
    db_url = f'postgresql://{login}:{password}@{host}:{port}/{database}'
    return db_url



def main(report_code):
    db_url = get_config('/Users/sergejnovikov/PycharmProjects/reportserver/treport/main.ini')

