import configparser
from lxml import etree

XSD_FILE_NAME = 'treports.xsd'

def get_config(path_to_inifile):
    config = configparser.ConfigParser()
    config.read(path_to_inifile)
    login = config.get('database', 'login')
    password = config.get('database', 'password')
    host = config.get('database', 'host')
    port = config.get('database', 'port')
    database = config.get('database', 'database')
    db_url = f'postgresql://{login}:{password}@{host}:{port}/{database}'
    path_to_params_reports_file = config.get('report', 'params_reports')
    return db_url, path_to_params_reports_file


def get_errors_validation(xml_doc, xsd_doc):
    xmlschema = etree.XMLSchema(xsd_doc)
    log = xmlschema.error_log
    #return xmlschema.assertValid(xml_doc)
    return xmlschema.assert_(xml_doc)



class Report():
    codeReport = ''
    nameReport = ''
    template = ''
    outDir = ''
    params_report = {}
    xml_validation_result : bool = None
    report_file_name = ''
    reportPages = {}
    '''reportPages - атрибут, в котором хранятся сведения о листе отчета. Атрибут является словарем и имеет следующие ключи:
    pageName - имя листа в xlsx-файле
    sqlFile - путь к файлу, в котором хранится SQL-запрос
    headerRows - количество строк на листе, которые выделены под заголовок
    ignoredColumns - список номеров колонок, которые необходимо игнорировать при формировании отчета
    sqlText - текст SQL-запроса, который сформирован на основании текста в файле с подставленными значениями параметров запроса
    sqlResult - результат выполнения SQL-запроса
    '''
    contentReport = None
    '''
    Контент сформированного отчета. 
    '''


    def __init__(self, report_code, path_to_params_reports_file):
        self.codeReport = report_code
        self.get_params_report(path_to_params_reports_file)

    def validate_params_report(self, xml_doc, xsd_doc):
        xmlschema = etree.XMLSchema(xsd_doc)
        return xmlschema.validate(xml_doc)


    def get_params_report(self, path_to_params_reports_file):

        def generate_file_name(filename_rule):
            return 'Итоговый файл.xlsx'

        xsd_f = open(XSD_FILE_NAME)
        xml_f = open(path_to_params_reports_file)
        xsd_doc = etree.parse(xsd_f)
        xml_doc = etree.parse(xml_f)
        self.xml_validation_result = self.validate_params_report(xml_doc, xsd_doc)
        if self.xml_validation_result:
            xml_root = xml_doc.getroot()[0]
            for item_report in xml_root:
                if item_report.attrib['codeReport'] == self.codeReport:
                    self.nameReport = item_report[0].text
                    self.template = item_report[1].text
                    self.outDir = item_report[2].text
                    file_name_rule = item_report[3]
                    self.report_file_name = generate_file_name(file_name_rule)

                    params = item_report[4]
                    for item_params in params:
                        parametr_code = item_params.attrib['id']
                        self.params_report[parametr_code] = {}
                        for parametr in item_params:
                            self.params_report[parametr_code][parametr.tag] = parametr.text

                    report_pages = item_report[5]
                    for item_page in report_pages:
                        page_code = item_page.attrib['codePage']
                        self.reportPages[page_code] = {}
                        for parametr_page in item_page:
                            self.reportPages[page_code][parametr_page.tag] = parametr_page.text





def main(report_code):
    db_url, path_to_params_reports_file = get_config('/Users/sergejnovikov/PycharmProjects/reportserver/treport/main.ini')
    report = Report(report_code, path_to_params_reports_file)

    if report.xml_validation_result:
        print(report.codeReport, report.nameReport, report.template)
        print(report.params_report)
        print(report.outDir, report.report_file_name)
        print(report.reportPages)
    else:
        print('Файл не корректен')

