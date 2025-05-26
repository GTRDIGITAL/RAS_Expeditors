import json

config_data = {
    "mysql": {
        "host": "192.168.1.222",
        "port": 3306,
        "user": "userAdmin",
        "password": "some_pass",
        "database": "efacturaferro"
    }
}

with open('config.json', 'w') as file:
    json.dump(config_data, file, indent=4)

print("Fișierul de configurare a fost creat cu succes!")
