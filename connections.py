import sys
import os

import Cassandra.main
import Mongo2.populate
import app
import Mongo2.mongo
import Dgraph.dgraph



def main():
    while True:

        app
        # Imprimir los menús
        menu_principal()
        menu_cassandra()
        menu_dgraph()
        menu_mongodb()

        choice = int(input("Seleccione una opción: "))

        if choice == 1:
            print("="*50)
            print("Cargando Datos CassandraDB...")
            Cassandra.main.load_data()  # Aquí llamamos a la función 'load_data()' dentro de Cassandra.main
            
            print("="*50)
            print("Cargando Datos MongoDB...")
            Mongo2.populate.main()

            print("="*50)
            print("Cargando Datos DgraphDB...")
            Dgraph.dgraph.load_data()
        
        elif choice == 0:
            print("Saliendo del programa...")
            exit(0)
        
        elif choice >= 2 and choice <= 18:
            choice = choice - 1
            Cassandra.main.main(str(choice))  # Aquí llamamos a la función 'main()' dentro de Dgraph.main
        
        elif choice >= 19 and choice <= 32:
            choice = choice - 18
            Dgraph.dgraph.main(choice)  # Aquí llamamos a la función 'main()' dentro de Dgraph.main
            
        elif choice >= 33 and choice <= 44:
            choice = choice - 32
            Mongo2.mongo.main(str(choice))
        else:
            print("Opción no válida. Intente de nuevo.")
        

# Menu principal
def menu_principal():
    print("\n--- MENÚ DE CONEXIONES ---")
    print("Seleccione la base de datos a la que desea conectarse:")
    print("1. Cargar datos de todas las bases de datos")
    print("0. Terminar programa")

# Menu para Cassandra
def menu_cassandra():
    print("\n--- MENÚ CASSANDRA ---")
    print("2. Crear keyspace y tablas")
    print("3. Insertar un post")
    print("4. Ver posts de un usuario")
    print("5. Cargar datos")
    print("6. Eliminar keyspace")
    print("7. Ver seguidores de un usuario")                  
    print("8. Ver posts guardados")                           
    print("9. Ver feed de usuario")                           
    print("10. Ver notificaciones")                            
    print("11. Ver top likers")
    print("12. Verificar si user_id existe en user_posts")
    print("13. Ver comentarios de un post")
    print("14. Ver likes de un post")
    print("15. Ver historial de login de un usuario")
    print("16. Ver posts guardados por un usuario")
    print("17. Ver vistas de un post")
    print("18. Ver top 5 usuarios con más publicaciones")

# Menu para Dgraph
def menu_dgraph():
    mm_options = {
        19: "Poblar base de datos",
        20: "Seguir usuario",
        21: "Dejar de seguir usuario",
        22: "Dar like a un post",
        23: "Comentar un post",
        24: "Ver usuarios seguidos",
        25: "Ver seguidores",
        26: "Ver comentarios en un post",
        27: "Ver seguidores mutuos",
        28: "Recomendar cuentas",
        29: "Recomendar cuentas por interacción",
        30: "Recomendar posts para interactuar",
        31: "Recomendar posts para compartir",
        32: "Borrar toda la data & schema",
    }

    print("\n--- MENÚ PRINCIPAL DGRAPH ---")
    for key in sorted(mm_options.keys()):
        print(f"{key}. {mm_options[key]}")

def menu_mongodb():
    print("\n==== MENU PRINCIPAL MONGODB ====")
    print("33. Consultar un perfil")
    print("34. Ver preferencias")
    print("35. Ver perfiles más usados")
    print("36. Ver perfiles generales por usuario")
    print("37. Agregar nuevo perfil")
    print("38. Modificar perfil")
    print("39. Eliminar perfil")
    print("40. Actualizar preferencias")
    print("41. Total de posts por usuario")
    print("42. Rankear perfiles por engagement")
    print("43. Buscar por red social")
    print("44. Mostrar el perfil más activo por usuario")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error: {}".format(e))