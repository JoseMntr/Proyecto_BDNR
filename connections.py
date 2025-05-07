import sys
import os

import Dgraph.dgraph

def menu():
    print("1. CassandraDB")
    print("2. MongoDB")
    print("3. DgraphDB")


def main():
    while True:
        menu()
        choice = input("Seleccione una opción (1-3): ")
        if choice == "1":
            print("Conectando a CassandraDB...")
            # Aquí puedes agregar la lógica para conectarte a CassandraDB
        elif choice == "2":
            print("Conectando a MongoDB...")
        elif choice == "3":
            print("Conectando a DgraphDB...")
            Dgraph.dgraph.main()  # Aquí llamamos a la función 'main()' dentro de Dgraph.main
        else:
            print("Opción no válida. Intente de nuevo.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error: {}".format(e))