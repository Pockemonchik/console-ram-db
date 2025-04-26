from ram_db import RamDB


def main():
    db = RamDB()
    print("\nДобро пожаловать в RamDB CLI!\n")
    print("----------------------------------------------------------")
    print("Список команд:\n")
    print("1. SET KEY VALUE (Добавляет или обновляет key-value пару)")
    print("2. GET KEY (Получение значения из БД по ключу)")
    print("3. UNSET KEY (Удаляет значние из БД)")
    print("4. FIND VALUE (Выводит сколько раз значение встречается в БД.")
    print("5. COUNTS VALUE (Выводит переменные для данного значения")

    print("6. END (закрывает приложение)")

    print("Введите команду: ")
    while True:
        try:
            command = input().strip()
            if command.split()[0].lower() == "set":
                db.set(command.split()[1], command.split()[2])
            elif command.split()[0].lower() == "get":
                print(db.get(command.split()[1]))
            elif command.split()[0].lower() == "unset":
                db.unset(command.split()[1])
            elif command.split()[0].lower() == "find":
                print(db.find(command.split()[1]))
            elif command.split()[0].lower() == "counts":
                print(db.counts(command.split()[1]))
            elif command.split()[0].lower() == "begin":
                db.begin_transaction()
            elif command.split()[0].lower() == "commit":
                db.commit_transaction()
            elif command.split()[0].lower() == "rollback":
                db.rollback_transaction()
            elif command.split()[0].lower() == "end":
                print("Заканчиваем работу CLI!")
                break
            else:
                print("Некорректная команда. Пожалуйста ознакомтесь с инструкцией")
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
