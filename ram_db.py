from typing import Any
import operator


class RamDB:
    """Основной класс БД"""

    def __init__(self) -> None:
        self.main_data: dict[str, Any] = {}  # Основное хранилище данных key-value
        self.transaction_stack: list[dict[str, Any]] = (
            []
        )  # Список незакоммиченных транзаций

    def begin_transaction(self) -> None:
        """
        Запускает новую транзакцию
        Добавление пустого словаря в стэк транзакций
        """
        self.transaction_stack.append({})

    def commit_transaction(self) -> None:
        """
        Коммитим изменения последней транзации в предыдущую
        или в сновную БД, если их больше нет
        """
        if not self.transaction_stack:
            raise Exception("Нет активных транзакций")

        if len(self.transaction_stack) == 1:
            # коммитим сразу в бд если нет больше тразацкиц в стеке
            self.main_data.update(self.transaction_stack.pop())
            # удалем все None value данные, которые пришли из транзакций
            for key, value in self.main_data.items():
                if value == None:
                    del self.main_data[key]
        else:
            # мержим транзацкцию в предыдущую
            last_transaction = self.transaction_stack.pop()
            self.transaction_stack[-1].update(last_transaction)

    def rollback_transaction(self) -> None:
        """
        Отменяет последнюю транзакцию
        """
        self.transaction_stack.pop()

    def get_current_db_state_with_transactios(self) -> dict[str, Any]:
        """
        Возвращает текущее состояние базы,
        такое если бы все транзацкции применились
        """
        result = self.main_data.copy()
        for transaction in self.transaction_stack:
            result.update(transaction)
        return result

    def set(self, key: str, value: Any) -> None:
        """
        Добавляет или обновляет key-value.
        Если есть активная транзацкия, то работает с ней.
        """
        if self.transaction_stack:
            self.transaction_stack[-1][key] = value
        else:
            self.main_data[key] = value

    def unset(self, key: str) -> None:
        """
        Удаляет, ранее установленную переменную.
        Если значение не было установлено, не делает ничего.
        Если есть активная транзацкия, то работает с ней.
        """
        if self.transaction_stack:
            # не удалаяем а ставим None что бы удалить при комите
            self.transaction_stack[-1][key] = None
        else:
            del self.main_data[key]

    def get(self, key: str) -> str:
        """
        Возвращает, ранее сохраненную переменную.
        Если такой переменной не было сохранено, возвращает NULL
        Сначала проходит по транзациям начиная с последней,
        потом по общей БД, выдает первое найденное значение
        """
        db_state = self.get_current_db_state_with_transactios()
        result = db_state.get(key)
        if result is None:
            return "NULL"
        return f"{result}"

    def find(self, value: Any) -> str:
        """
        Выводит найденные установленные
        переменные для данного значения.
        """
        db_state = self.get_current_db_state_with_transactios()
        keys = [key for key, data in db_state.items() if value == data]
        result_str = " ".join(keys)
        return f"{result_str}"

    def counts(self, value: Any) -> str:
        """
        Показывает сколько раз данные значение
        встречается в базе данных.
        """
        db_state = self.get_current_db_state_with_transactios()
        count = operator.countOf(db_state.values(), value)
        return f"{count}"
