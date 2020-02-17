import sys

from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor


def ncreif_web_service():
    # service description
    url = 'https://www.ncreif.org/ncreif/webservice/querybuilder.asmx?WSDL'
    user_name = 'Wendy.wang@clarionpartners.com'
    user_password = '661218SIsi!!'

    imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
    imp.filter.add('http://tempuri.org/')

    # args
    p_DataTypeId = int(sys.argv[1])
    p_SelectQuery = sys.argv[2]
    p_WhereClause = sys.argv[3]
    p_GroupbyClause = sys.argv[4]
    p_QueryData = int(sys.argv[5])
    p_UserName = user_name
    p_Password = user_password

    client = Client(url, doctor=ImportDoctor(imp))

    # result from server
    result = client.service.ExecuteQuery(p_DataTypeId, p_SelectQuery, p_WhereClause, p_GroupbyClause, p_QueryData,
                                         p_UserName, p_Password)
    import ipdb;ipdb.set_trace()
    return result


if __name__ == '__main__':
    ncreif_web_service()
