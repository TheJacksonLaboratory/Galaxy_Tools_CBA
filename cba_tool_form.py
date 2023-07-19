import os

def get_dir_options(test_dir):
    # test_dir = str("/shares/MNBF/Testing Data/TEMP for Spont Alt Tracks/")
    # print(i)
    # return tree
    root, dirs, files = next(os.walk(test_dir))
    # dir_values = [("a","a",0)]
    dir_values = [(value, test_dir + value, 0) for value in sorted(dirs)]
    dir_values.insert(0,("","",0))
    # print (str(dir_values))
    return dir_values

if __name__ == '__main__':
    # get_dir_options("/shares/MNBF/Testing Data/TEMP for Spont Alt Tracks/")
    print(get_dir_options())

