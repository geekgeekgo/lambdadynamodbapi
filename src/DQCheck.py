import re
import json


def dqcheck(body):
    err_msg=[]
    DQ=True
    if(body['org_name'].strip() =='' or len(body['org_name'])<3):
        DQ=False
        err_msg.append('ERR_ORG_NAME')
    if(int(body['org_id'].strip()) < 1 ):
        DQ=False
        err_msg.append('ERR_ORG_ID')
    if(body['addr_ln_1'].strip()== '' ):
        DQ=False
        err_msg.append('ERR_ADDR_LN_1')
    if (body['city'].strip()==''):
        DQ=False
        err_msg.append('ERR_ADDR_CITY')
    if(body['phn_nbr'].strip()!=''):
        if((bool(re.search('[a-zA-Z]', body['phn_nbr'].strip()))) or len(body['phn_nbr'].strip())<10):
            DQ=False
            err_msg.append('ERR_PHN_NBR')
    if(body['email'].strip()!=''):
        if(not bool(re.search('[@]',body['email'].strip() )) or len(body['email'].strip())<3):
            DQ=False
            err_msg.append('ERR_EADDR')
    return DQ,str(err_msg)