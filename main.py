from mpi4py import MPI
from filelock import FileLock
from collections import OrderedDict
import os
import math
import re
import random

input_directory = ""
output_directory = ""

"""
brief   : Functie care citeste datele de intrare
"""
def read_input_arguments():
    global input_directory, output_directory
    try:
        with open("inputArgs.txt",'r') as f:
            data = f.read()
            args = data.split(" ")

            input_directory = args[0]
            output_directory = args[1]

            if os.path.exists(os.getcwd() + "\\" + output_directory) == False:
                os.mkdir(os.getcwd() + "\\" + output_directory)

    except FileNotFoundError:
            print("Could not find the file")


"""
brief   : Functie care elimina caracterele nedorite din text
params  : string - textul ccare urmeaza sa fie procesat
returns : Textul dupa procesare
"""
def remove_characters(string):
    return re.sub(r"[^a-zA-Z]+", " ", string).strip().lower()


"""
brief   : Functie care pregateste datele pentru mapare
params  : files - fisierele de care se ocupa procesul
"""
def pre_processing(files):
    for file in files:
        try:
            with open(file,'r',encoding="unicode_escape") as f:
                data = f.read()
            with open(file,"w+",encoding="unicode_escape") as f:
                f.write(remove_characters(data))
        except FileNotFoundError:
            print("Could not find the file")


"""
brief   : Functie care scrie in fisierele intermediare datele
params  : fileID - fisierul in care exista intrarile din distinct_words_dict
          distinct_words_dict - dictionarul cu key=cuvant si value=numarul de aparitii al cuvantului
"""
def make_pairs(fileID, distinct_words_dict):

    current_directory = os.getcwd() + "\\map\\"

    for word in distinct_words_dict:

        file_path = current_directory + word[0] + ".txt"
        
        lock = FileLock(file_path + ".lock")
        with lock:
            with open(file_path, "a") as f:
                occurence = distinct_words_dict[word]

                #scriu in fisier word docID count 
                to_be_written = word + " " + fileID + " " + str(occurence) + "\n"
                f.write(to_be_written)


"""
brief   : Functie care stabileste datele de interes
params  : files - fisierele de care se ocupa un proces
"""
def mapping(files):
    for file in files:
        fileID = os.path.basename(file)[:len(os.path.basename(file))-4]

        try:
            with open(file,'r',encoding="unicode_escape") as f:
                data = f.read()
                
                words_dict = {}

                words = set(data.split(" "))
                for word in words:
                    words_dict[word] = 1

                make_pairs(fileID, words_dict)    

        except FileNotFoundError:
            print("Could not find the file")


"""
brief   : Functie care imi returneaza fisierele de care se va ocupa un proces
params  : no_of_processes - numarul total de procese disponibile
          rank            - rangul unui proces 
          directory       - directorul unde se afla fisierele
returns : Fisierele de care se va ocupa procesul cu rangul rank
"""
def get_process_files(no_of_processes, rank, directory):
    files = []
    number_of_files_per_process = 0
    no_of_available_processes = 0

    dir_name = os.getcwd() + "\\" + directory
    list_of_files = os.listdir(dir_name)
    
    random.shuffle(list_of_files)
    #fiecare proces se ocupa de fisierele de la (rank-1)*number_of_files_per_process -> rank * number_of_files_per_process
    #in afara de ultimul proces care se va ocupa de toata lista ramasa incepand cu (rank-1)*number_of_files_per_process
    
    if rank > ((no_of_processes - 1) // 2):
        #sunt in etapa de reducere
        no_of_available_processes = no_of_processes - ((no_of_processes - 1) // 2) - 1
        number_of_files_per_process = math.floor(len(list_of_files)/no_of_available_processes)

        #pentru a scala rangul procesului sa pot imparti fisierele in acelasi mod (spre ex procesul 4 va avea rangul 1, procesul 5 rangul 2 etc)
        rank = rank - ((no_of_processes - 1) // 2)

    else:
        #sunt in etapa de mapare
        no_of_available_processes = (no_of_processes - 1) // 2
        number_of_files_per_process = math.floor(len(list_of_files)/no_of_available_processes)

    
    if rank == no_of_available_processes:
        files = list_of_files[((rank - 1) * number_of_files_per_process):]
    else:
        files = list_of_files[((rank - 1) * number_of_files_per_process):(rank * number_of_files_per_process)]
    
    files = [dir_name + "\\" + x for x in files]
   
    return files


"""
brief   : Functie care calculeaza numarul de aparitii al fiecarui cuvant si care sorteaza dupa termen si fileID
params  : files - fisierele de care se ocupa un proces
"""
def reducing(files):
    for file in files:
        try:
            with open(file,'r',encoding="unicode_escape") as f:
                lines = f.readlines()

                distinct_words_dict = {}

                for line in lines:
                    entry = line.split(" ")
                    key_tuple = (entry[0], int(entry[1]))
                    
                    if key_tuple in distinct_words_dict:
                        distinct_words_dict[key_tuple] += int(entry[2])
                    else:
                        distinct_words_dict[key_tuple] = int(entry[2])

                sorted_dictionary = OrderedDict(sorted(distinct_words_dict.items()))

                final_operation(sorted_dictionary)
                
        except FileNotFoundError:
            print("Could not find the file")


"""
brief   : Functie care obtine rezultatul final al procesarii dorite
params  : sorted_dictionary - dictionarul sortat in functie de criterii
"""
def final_operation(sorted_dictionary):

    final_dictionary = {}
    for key in sorted_dictionary:
        if key[0] in final_dictionary:
            final_dictionary[key[0]] += "<" + str(key[1]) + ", " + str(sorted_dictionary[key]) + "> "  
        else:
            final_dictionary[key[0]] = "<" + str(key[1]) + ", " + str(sorted_dictionary[key]) + "> "  
    
    for key in final_dictionary:
        file_path = os.getcwd() + "\\" + output_directory + "\\" + key[0] + ".txt"
        #file_path = os.getcwd() + "\\" + output_directory + "\\" + key + ".txt"
        lock = FileLock(file_path + ".lock")

        with lock:
            with open(file_path, "a") as f:
                to_be_written = key + ": " + final_dictionary[key] + "\n"
                f.write(to_be_written)


def main():
    global input_directory
    read_input_arguments()
    comm = MPI.COMM_WORLD
    
    #rangul procesului curent
    rank = comm.Get_rank()

    #numarul de procese disponibile
    no_of_processes = comm.Get_size()
   
    if no_of_processes < 3:
        if rank == 0:
            print("Numarul de procese trebuie sa fie cel putin 3")
        exit()

   
    if rank == 0:
        i = 1
        while i <= (no_of_processes - 1) // 2:
            files = get_process_files(no_of_processes, i, input_directory)
            comm.send(files, dest=i)
            i = i + 1
        
        #astept mesaj de la toate procesele care se ocupa de mapare
        i = 1
        while i <= (no_of_processes - 1) // 2:
            message = comm.recv(source=i)
            print("Procesul 0 a primit: " + message)
            i = i + 1

        #incep reducerea
        i = (no_of_processes - 1) // 2 + 1
        while i < no_of_processes:
            files = get_process_files(no_of_processes, i, "map")
            comm.send(files, dest=i)
            i = i + 1
        
        #astept confirmarea ca este gata reducerea
        i = (no_of_processes - 1) // 2 + 1
        while i < no_of_processes:
            message = comm.recv(source=i)
            print("Procesul 0 a primit: " + message)
            i = i + 1

    else:
        #vreau ca prima jumatate de procese sa se ocupe de mapare si restul de procesare
        if rank <= math.floor((no_of_processes / 2)):

            files = comm.recv(source=0)
            print("Process " + str(rank) + " started mapping")
           
            pre_processing(files)
            mapping(files)
            
            comm.send("Process " + str(rank) + " finished mapping", dest=0)
        else:

            files = comm.recv(source=0)

            print("Process " + str(rank) + " started reducing")
            reducing(files)

            comm.send("Process " + str(rank) + " finished reducing", dest=0)
       
    
main()

           