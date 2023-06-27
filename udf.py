import re
def extract_phone_number(text):
    """
    Extracts phone numbers from text using regular expressions. use pipe | for either or give multiple expressions
    """
    pattern = r"\d{3}-\d{3}-\d{4}|\d{3} \d{3} \d{4}|\d{3}\d{3}\d{4}"
    matches = re.findall(pattern, text)
    return matches

list=["my phone number is 226-961-5720","my name is karthick phone number is 226 754 5485", "nihil is 2269856845"]
for i in list:

    print(f"{extract_phone_number(i)}")


def my_function(*kids):
  print("The youngest child is " + kids[2])

my_function("Emil", "Tobias", "Linus")


#conditional masking

# Define a sample list of lists
data = 126-85-8975
x = 126-85-8975

print(str(x))
# Define the condition function
def is_ssn(data):
    """
    Returns True if the value is a Social Security Number.
    """
    pattern = r"\d{3}-\d{2}-\d{4}"
    return bool(re.match(pattern, str(data)))


def conditional_masking(df, column, condition, mask_char="*"):
    """
    Applies conditional masking to a DataFrame column based on a condition.
    """
    df[column] = df[column].apply(lambda x: mask_char * len(str(x)) if condition(x) else x)
    return df

is_ssn1=conditional_masking(data,data,is_ssn)
print(is_ssn1)

from pyspark.sql.functions import udf

def mask_email(email):
    at_index = email.index('@')
    print(f"{at_index}")
    return email[0] + "*" * (at_index-2) + email[at_index-1:]

mask_email_udf = udf(mask_email)
out= mask_email_udf("harish@gmail.com")
print(f"{out}")

def mask_mobile(mobile):
    return mobile[0] + "*" * (len(mobile) - 2) + mobile[-1]

mask_email_udf = udf(mask_email)
mask_mobile_udf = udf(mask_mobile)


mobile=9502561222
out=mobile[0] + "*" * (len(mobile) - 2) + mobile[-1]
print(f"{out}")