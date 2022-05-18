from treport.report import main

if __name__ == '__main__':
    parameters = {'p_start_date': '01.01.2022', 'p_end_date': '31.01.2022', 'p_is_ks': 'true', 'p_is_app': 'true'}
    main('schl_foms_89n_only', parameters)