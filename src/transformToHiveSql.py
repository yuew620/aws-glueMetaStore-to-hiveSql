import json
from types import SimpleNamespace

def convert(line):
    info = json.loads(line, object_hook=lambda d: SimpleNamespace(**d))
    return f'\nCREATE {get_external(info)} TABLE IF NOT EXISTS {get_table_name(info)} (' \
           f'\n{get_columns(info)}' \
           f'\n)' \
           f'{partitioned_by(info)}' \
           f'\nSTORED AS' \
           f'\n{get_inputformat(info)}' \
           f'\n{get_outputformat(info)}' \
           f'\n{get_location(info)}' \
           f'\n;'

def get_table_name(info):
    return f'{info.database}.{info.item.name}'



def get_columns(info):
    cols = map(lambda col: f'{col.name} {col.type}', info.item.storageDescriptor.columns)
    return ',\n'.join(cols)

def partitioned_by(info):
    if len(info.item.partitionKeys) == 0:
        return ''
    else:
        return f'\nPARTITIONED BY ({get_partitions(info)})'

def get_external(info):
    if info.item.tableType=="EXTERNAL_TABLE":
        return f'External'
    else:
        return f''

def get_location(info):
    return f'LOCATION \'{info.item.storageDescriptor.location}\''

def get_inputformat(info):
    df = ''
    try:
        df = f'INPUTFORMAT \'{info.item.storageDescriptor.inputFormat}\''
    except Exception as e:
        print("erro",e)
    return df

def get_outputformat(info):
    df = ''
    try:
        df =  f'OUTPUTFORMAT \'{info.item.storageDescriptor.outputFormat}\''
    except Exception as e:
        print("erro",e)
    return df

def get_partitions(info):
    pars = map(lambda par: f'{par.name} {par.type}', info.item.partitionKeys)
    return ', '.join(pars)




if __name__ == "__main__":

    f = open('/Users/***/Documents/code/aws-glue-metastore-to-hivesql/resource/tables/sampleTable.json', 'r')
    outF = open('/Users/***/Documents/code/aws-glue-metastore-to-hivesql/resource/tables/Output.txt', 'w')
    lines = f.readlines()
    for line in lines:
        content = convert(line)
        print(content)
        outF.write(content)