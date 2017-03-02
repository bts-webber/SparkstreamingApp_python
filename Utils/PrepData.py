#coding=utf-8
import string,csv,random
def Sql_Special_Word(data):
    #特征1：特殊关键字
    sql_special_words = ['and', 'or', 'xor', 'sysobjects', 'msysobjects', 'version', 'substr', 'substring', 'len',
                         'length', 'exists', 'mid', 'asc', 'inner', 'join', 'xp_cmdshell', 'exec', 'having', 'group by',
                         'back up', 'union select', 'order by', 'information', 'schema', 'load_file',
                         'load data infile', 'into outfile', 'into dumpfile']
    data=data.lower()
    for special_word in sql_special_words:
        if special_word in data:return 1
    return -1
def Xss_Special_Word(data):
    #特征1：特殊关键字
    xss_special_words = ['script', 'alert', 'prompt', 'location', 'hash', 'src', 'href', '@ import', 'eval',
                         'xmlhttprequest', 'activxobject']
    data=data.lower()
    for special_word in xss_special_words:
        if special_word in data:return 1
    return -1
def Special_Char_rate(data):
    #特征2：特殊字符频率
    special_chars = '！"#%&\':;<>=?@[]\\/{}|$,*+-.'
    n=0
    for special_char in special_chars:
        n+=data.count(special_char)
    return float(n)/len(data)
def Special_preChar_rate(data):
    #特征3：特殊前缀字符频率
    special_pre_chars = ['&#', '&#x', '\\', '\\x', '\\u', '%']
    n = 0
    for special_char in special_pre_chars:
        n+=data.count(special_char)
    return float(n)/len(data)
def Upper_rate(data):
    #特征4：大字字母频率
    n=0
    for char in data:
        if char in string.uppercase:n+=1
    return float(n)/len(data)
def Number_rate(data):
    #特征5：数字频率
    n = 0
    for char in data:
        if char in "0123456789":n+=1
    return float(n)/len(data)
def Space_rate(data):
    #特征6：空格频率
    n = 0
    for char in data:
        if char==" ":
            n+=1
    return float(n)/len(data)
def SqlVector(data):
    value=[
        Sql_Special_Word(data),
        Special_Char_rate(data),
        Special_preChar_rate(data),
        Upper_rate(data),
        Number_rate(data),
        Space_rate(data)
    ]
    return value
def XssVector(data):
    value=[
        Xss_Special_Word(data),
        Special_Char_rate(data),
        Special_preChar_rate(data),
        Upper_rate(data),
        Number_rate(data),
        Space_rate(data)
    ]
    return value
def Get_Sql_ValueFromFile(path):
    f=file(path,"rb")
    reader=csv.DictReader(f)
    return [SqlVector(data["payload"]) for data in reader ]
def Get_Xss_ValueFromFile(path):
    f=file(path,"rb")
    reader=csv.DictReader(f)
    return [XssVector(data["payload"]) for data in reader ]
def Merge_Sql_data(sql_exploit_db,normal_data):
    data_num = len(sql_exploit_db) + len(normal_data)
    index = 0
    data, label = [0] * data_num, [0] * data_num
    for d in sql_exploit_db:
        data[index] = d
        label[index] = "SqlInjection"
        index += 1
    for d in normal_data:
        data[index] = d
        label[index] = "Normal"
        index += 1
    return data,label
def Merge_Xss_data(xss_xssed,normal_data):
    data_num = len(xss_xssed) + len(normal_data)
    index = 0
    data, label = [0] * data_num, [0] * data_num
    for d in xss_xssed:
        data[index] = d
        label[index] = "Xss"
        index += 1
    for d in normal_data:
        data[index] = d
        label[index] = "Normal"
        index += 1
    return data,label
def Train_Test(data,label):
    data_num=len(data)
    train_data_num = data_num / 3 * 2
    test_data_num = data_num - train_data_num
    test_data_index = random.sample(range(data_num), test_data_num)
    train_data_index = range(data_num)
    for i in test_data_index:
        train_data_index.remove(i)
    train_data, train_label = [0] * train_data_num, [0] * train_data_num
    test_data, test_label = [0] * test_data_num, [0] * test_data_num
    for i in xrange(train_data_num):
        train_data[i] = data[train_data_index[i]]
        train_label[i] = label[train_data_index[i]]
    for i in xrange(test_data_num):
        test_data[i] = data[test_data_index[i]]
        test_label[i] = label[test_data_index[i]]
    return train_data,train_label,test_data,test_label
def TestError(model,test_data,test_label):
    pre_result = model.predict(test_data)
    test_data_num=len(test_data)
    error_num = 0
    sql_error_num = 0
    xss_error_num = 0
    normal_error_num = 0
    for i in xrange(test_data_num):
        if pre_result[i] != test_label[i]:
            error_num += 1
            if test_label[i] == "SqlInjection":
                sql_error_num += 1
            elif test_label[i] == "Xss":
                xss_error_num += 1
            else:
                normal_error_num += 1
    return error_num,sql_error_num,xss_error_num,normal_error_num

