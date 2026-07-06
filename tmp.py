from enum import Enum

class Status(Enum):
    ACTIVE = "активный"
    INACTIVE = "неактивный"
    PENDING = "в ожидании"
    DELETED = "удален"

# Использование
print(Status.ACTIVE.value)  # "активный"
print(Status["ACTIVE"].value)  # "активный"

# Получить все значения
for status in Status:
    print(f"{status.name}: {status.value}")