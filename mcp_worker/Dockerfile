# Используем официальный стабильный и легковесный образ Python на базе Debian Bookworm
FROM python:3.11-slim-bookworm

# Настраиваем переменные окружения, чтобы Python не буферизировал логи 
# и не плодил мусорные файлы кэша .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ЖЕСТКАЯ ИНЖЕНЕРИЯ: Насильно выставляем системную локаль UTF-8 на уровне Linux.
# Больше никакой PowerShell не сможет навязать нам кривую кодировку!
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Сначала копируем только файл зависимостей (для эффективного кэширования слоев Docker)
COPY requirements.txt .

# Обновляем pip и устанавливаем пакеты без сохранения локального кэша колес (wheel),
# чтобы минимизировать размер итогового контейнера
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код воркера (server.py, core/, data/) в контейнер
COPY . .

# Наш HTTP-сервер "рук" будет слушать 5011 порт
EXPOSE 5011

# Запускаем наш сервер инструментов
CMD ["python", "server.py"]