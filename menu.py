import questionary

def main():
    respuesta = questionary.select(
        "Elige una opción:",
        choices=["Opción 1", "Opción 2", "Opción 3", "Salir"]
    ).ask()

    print(f"Has seleccionado: {respuesta}")

if __name__ == "__main__":
    main()
