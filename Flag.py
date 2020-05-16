def flag(code):
    OFFSET = 127462 - ord('A')
    code = code.upper()
    return chr(ord(code[0]) + OFFSET) + chr(ord(code[1]) + OFFSET)