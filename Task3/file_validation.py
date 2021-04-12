import csv
import sys as s


# Function for reading a configuration file(information file) that contains information of the file
def config_read_in_dic():
    '''Function to obtain the values of external configuration file in dict form that are useful in comparing with the csv file records'''
    filename = "ext.config"
    contents = open(filename).read()
    config = eval(contents)
    header_record_position = config["header_record_position"]
    footer_record_position = config["footer_record_position"]
    no_of_columns = config["no_of_columns"]
    no_of_records = config["no_of_records"]
    dic = {"Header Record Position": header_record_position, "Foot Record Position": footer_record_position,
           "No OfColumns": no_of_columns, "No Of Records": no_of_records}
    return dic


config_data_of_dic = config_read_in_dic()
# print(config_data_of_dic, 'are external file values')


def Validate_csv(config_data_of_dic,file):
    '''Function for validating a csv file and fetching the desired results like info about header,footer,Column_length,number_of_rows'''
    f = open(file, 'r')
    r = csv.reader(f)
    data = list(r)
    # print(type(data))
    footer = len(data)

    # header=len(data)-len(data[4:])
    # print("footer position ",footer)
    # print("header is ",header)

    # for line in data:
    #     print(line)
    # for word in line:
    #     print(word, end='\t')
    # print()

    # print(config_data)
    # tot_columns=int(config_data[2])

    tot_columns = int(config_data_of_dic["No OfColumns"])
    # print('total number of columns are ',tot_columns)

    header = get_header(data, tot_columns)
    # print('header position is ', header, ' type of header is ', type(header))

    no_of_columns = get_no_of_columns(data)
    # print('no of columns are ',no_of_columns)

    no_of_rows = get_no_of_rows(data, header, footer)
    # print('no_of_rows are ',no_of_rows)

    csv_pos_values = [header, footer, no_of_columns, no_of_rows]
    # config_data=list(config_data)//Not required (optional)

    # print('csv_pos_values ',csv_pos_values)
    # print('config_data ',config_data)

    column_names = get_header_column_names(data, tot_columns)
    # print("column names ",column_names)

    # print(config_data_of_dic, 'are external file values')

    csv_file_information = get_csv_file_info(data)
    # print(csv_file_information, ' are csv values')

    file_validation_result = validate_file(csv_pos_values, config_data_of_dic)
    compare_external_with_csv_info_result = compare_ext_with_csv(csv_file_information, config_data_of_dic)

    # print(compare_external_with_csv_info_result, 'result')
    with open('log.txt', 'a') as file:
        print('file name is ',file_name,file=file)
        print(csv_file_information, ' are csv values', file=file)
        print(config_data_of_dic, 'are external file values', file=file)
        print("column names ", column_names, file=file)
    print(csv_file_information, ' are csv values')
    print(config_data_of_dic, 'are external file values')
    print("column names ", column_names)
    if file_validation_result == True and compare_external_with_csv_info_result==True:
        with open('log.txt','a')as file:
            print("Voila!!! You are done with File validation!!!!", file=file)
            print("---Now lets validate data inside it---", file=file)
        print("Voila!!! You are done with File validation!!!!")
        print("---Now lets validate data inside it---")
        records_data = data[header:footer - 1]
        validate_data(records_data, header + 1,csv_file_information,column_names)
    else:
        with open('log.txt','a') as file:
            print("File is missing with some data or File may be inappropriate", file=file)
            print("-------------------------------------------",file=file)
        print("File is missing with some data or File may be inappropriate")


def compare_ext_with_csv(csv_info, config_data_of_dic):
    for items in config_data_of_dic.items():
        if (items in csv_info.items() and config_data_of_dic.items() == csv_info.items()):
            # print(items)
            pass
        else:
            # print("it is bad file", items)
            return False
    else:
        # print("good file")
        return True


def get_header_column_names(data, tot_columns):
    '''Function for getting the header record/row position(value)'''
    for line in data:
        le = 0
        for word in line:
            if word != '':
                le = le + 1
        if le == tot_columns:
            st = line
            return st


def get_header(data, tot_columns):
    '''Function for getting the header record/row position(value)'''
    header = 0
    for line in data:
        le = 0
        for word in line:
            if word != '':
                le = le + 1
        if le == tot_columns:
            st = line
            # print('st is',st)
            for line in data:
                li = line
                if li == st:
                    header = header + 1
                    return header
                header = header + 1
    return header

    #         or
    # header = 0
    # st = data[4]
    # for line in data:
    #     li=line
    #     if li==st:
    #         header=header+1
    #         return header
    #     header=header+1


def get_no_of_rows(data, header, footer):
    '''Function that returns the value of number of records that are present in the csv file which are valid rows'''
    no_of_rows = 0
    # print(header,'header value')
    for line in data[header + 1:footer]:
        if len(line) == len(data[4]):
            no_of_rows = no_of_rows + 1
    return no_of_rows

    # no_of_rows = 0
    # for line in data[header + 1:footer]:
    #     le=0
    #     for word in line:
    #         if word != '':
    #             le=le+1
    #     if le == len(data[4]):
    #         no_of_rows = no_of_rows + 1
    # return no_of_rows


def get_no_of_columns(data):
    '''Function that returns the value of number of columns that are present in a given csv file'''
    return len(data[4])


def get_csv_file_info(data):
    for i in data[:4]:
        # print('i is ', i)

        csv_file_values = {}

        for iteration, line in enumerate(data[:4]):
            sp = list(line)
            if sp == data[iteration]:
                for word in sp:
                    if word != '':
                        # print(word)
                        def Convert(string):
                            li = list(string.split("="))
                            return li

                        list1 = Convert(word)

                        # print(list1)
                        def Convert(lst):
                            res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
                            return res_dct

                        dict1 = Convert(list1)
                        # print(dict1)
                        csv_file_values.update(dict1)
        return csv_file_values


def validate_file(csv_pos_values, config_data):
    '''Function that validates a file based on the results obtained from configuration_file and csv_position_values'''
    # print(config_data)
    for iteration, i in enumerate(config_data):
        # Also we can use enumerate function for getting counter of current runnning for loop
        # without explicitly declaring a counter variable
        conf_val = int(config_data[i])
        # print(type(conf_val), conf_val)

        if csv_pos_values[iteration] == conf_val:
            pass
        else:
            # print("bad file")
            return False
    else:
        # print("good file")
        return True


def validate_data(records_data, header,csv_file_information,column_names):
    '''Function that validates the Data of every cell of a record(row)'''
    with open('log.txt', 'a') as file:
        # print(records_data)
        for row_line, line in enumerate(records_data):
            row_line = row_line + header
            # print(line)
            counter = 0
            for iteration, word in enumerate(line):
                # enumerate is used to get count of number of iterations of current for loop
                # print(word, 'type is ', type(word))
                if word.isdigit():
                    # print("digit")
                    word = int(word)
                    is_digit_greater_than_zero = validate_digit(word)
                    if iteration != 0 and is_digit_greater_than_zero == True:
                        validate_digit_boundaries(word, iteration)
                elif iteration == 1 and word != '':
                    validate_First_Name(word, iteration, row_line)

                validate_cell(word, iteration, row_line)
            print("======================================")
        print('------------------------------------------',file=file)


def validate_First_Name(word, iteration, row_line):
    '''Function that checks whether a strings first character is in uppercase or not'''
    if word[0].isupper():
        pass
        # print(iteration,' ',word,' is valid')
    else:
        # print(word.capitalize(),' is our capital')
        with open("log.txt", 'a') as f:
            print('in row number ', row_line, 'cell number ', iteration + 1, ' ', word, ' is not valid',file=f)
        print('in row number ', row_line, 'cell number ', iteration + 1, ' ', word, ' is not valid')


def validate_cell(val, iteration, row_line):
    '''Function that checks if any cell is null'''
    if val != '':
        pass
    else:
        with open("log.txt",'a') as f:
            print("in row", row_line, "cell number ", iteration + 1, " value is null",file=f)
        print("in row", row_line, "cell number ", iteration + 1, " value is null")


def validate_digit(val):
    '''Function that checks whether a value is greater than zero or not'''
    if val > 0 and type(val) == int:
        return True


def validate_digit_boundaries(val, index_pos):
    '''Function that checks whether the value is between the range of 0 to 100(marks)'''
    if val > 0 and val <= 100:
        print(index_pos + 1, "valid marks")

# file="studentdetails.csv"
file_name = s.argv[1]
Validate_csv(config_data_of_dic,file_name)
