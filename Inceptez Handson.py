#count of vowels
st="my name is harish"
stsplit=st.split(" ")
stlist = list(stsplit)
print(stlist)
count =0
for i in stlist:
    if(i[0]=="a"
         or
    i[0]=="e"
        or
    i[0]=="i"
         or
     i[0]=="o"
         or
        i[0]=="u"):
        count=count+1
print(f"count is {count}")

#the average of list

list=[1,2,3,4,5,6,7,8,9,10]
string="harish"
sum=0
for i in list:
    sum=sum+i
    noe=len(list)
    avg=sum/noe
strlen=len(string)
print(f"{strlen}")
print(f"{noe}")
print(f"the average of list is {avg}")


#positional Masking

entername=input()
st2 = entername[0:2]
mid = "*"*(len(entername)-2)
ls2 = entername[::-2]
out = st2 + mid + ls2
print(out)



import re

def extract_phone_number(text):
    """
    Extracts phone numbers from text using regular expressions.
    """
    pattern = r"\d{3}-\d{3}-\d{4}"
    matches = re.findall(pattern, text)
    return matches

extract_phone_number("my phone number is 226-961 5720")