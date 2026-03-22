import os


# Specify the path to your text file
file_path = r"P-graph input file.txt"
# Open the file in read mode
os.system('cls')


with open(file_path, 'r') as file:
    # Read each row in the file
    max_length = 0
    line_number = 1
    errors =[]
    for line in file:
        #print(line.strip())
        line_length = len(line.strip())
        if line_length > max_length:
            max_length = line_length
 
        if len(line.strip()) <=15 and len(line.strip()) !=0 and line.strip()[0:2] !="ME" and line.strip()[0:5] !="opera" and line.strip()[0:2] !="ma" and line.strip()[0:2] !="de" and line.strip()[0:2] !="ti" and line.strip()[0:2] !="mo":
            errors.append(line.strip())
            print(line.strip())  # Use .strip() to remove any leading/trailing whitespace including newlines
        line_number += 1

    if errors==[]:
        print(f"Done checking {line_number} of lines.\nYour code looks fine!\nLongest line has {max_length} characters.")

    elif errors!=[]:
        print(f"Done checking {line_number} of lines.\nError in the code.")


