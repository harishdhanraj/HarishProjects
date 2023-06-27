mob=input()
first_one = mob[0:1]
middle_char = "*"*(len(mob)-2)
last_one = mob[len(mob)-1:]
out = first_one + middle_char + last_one
print(f"{out}")

email = input()
atindex = email.index("@")
atdot = email.index(".")
print(f"{atindex}")
print(f"{atdot}")


def mask()
first_one = email[0:1]
middle_char = "*"*(atindex-2)
last_one = email[atindex-1:0]
lastmin= "*"*(atdot-atindex-4)
com=email[atdot-3:]

out = first_one + middle_char + last_one + lastmin + com
print(f"{out}")
