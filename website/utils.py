def extract_name_from_email(email):
    local_part = email.split('@')[0]
    name_parts = local_part.replace('.', ' ').replace('-', ' ').split(' ')
    formatted_name = ' '.join([part.capitalize() for part in name_parts])
    return formatted_name
