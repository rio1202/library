import requests
import psycopg2
from tqdm import tqdm
from pyquery import PyQuery
from urllib.parse import urlparse
import fitz
from gpt4all import GPT4All
from datetime import datetime
import json
import re
import os
import time
import logging

DB_CONFIG = {
    "dbname": "book_scraper",
    "user": "postgres",
    "password": "root",
    "host": "localhost",
    "port": "5432",
}

max_books = 15  # Максимальное количество книг для парсинга
batch_size = 15  # Размер пакетной обработки
LOG_FILE = "parser.log"  # Файл для логов

MODEL_NAME = "orca-mini-3b.gguf" # Модуль локальной версии AI

MODEL_PATH = os.path.expanduser("~/.cache/gpt4all/")


logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

# Дублируем сообщения и в консоль и в лог-файл
def log(msg):
    print(msg)
    logging.info(msg)


# Инициализируем базу данных в ней мета, которые парсил с сайта и данные ..._parsed, которые должна была парсить AI, далее я хотел сравнивать полученные значения. Но надо время, чтобы это все доделать.
def init_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS books")
    cursor.execute("""CREATE TABLE books
                      (
                          id                 SERIAL PRIMARY KEY,  
                          book_title         TEXT,                
                          book_author        TEXT,                
                          book_link          TEXT UNIQUE,         
                          book_body          BYTEA,               
                          book_downloaded    BOOLEAN   DEFAULT FALSE, 
                          book_title_parsed  TEXT,                
                          book_author_parsed TEXT,                
                          book_year_parsed   TEXT,                
                          created_at         TIMESTAMP DEFAULT NOW() 
                      )""")
    conn.commit()
    conn.close()
    log("✅ База данных инициализирована")

# Парсинг метаданных книг с сайта
def parse_book_metadata(max_books):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    book_counter = 0
    page_num = 2  # Стартовая страница каталога начинается с 2, т.к. первая страница Json отличается от всех последующих отсутствием data[]

    try:
        while book_counter < max_books:
            try:
                response = requests.get(
                    f"https://www.mann-ivanov-ferber.ru/catalog/?apimode=1&page={page_num}",
                    timeout=15
                )
                response.raise_for_status()
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                log(f"❌ Ошибка при загрузке страницы {page_num}: {e}")
                time.sleep(5)
                continue

            try:
                data = response.json()
            except json.JSONDecodeError:
                log(f"❌ Не удалось распарсить JSON на странице {page_num}")
                page_num += 1
                continue

            # Извлечение списка книг
            books = data.get("products", [])

            if not books:
                log("ℹ️ Больше книг не найдено")
                break

            for book in books:
                if book_counter >= max_books:
                    break

                book_title = book.get("title", "").strip()
                book_author = book.get("author_name", "").strip()
                book_url = book.get("url", "")

                if not book_title or not book_url:
                    continue

                try:
                    page = requests.get(book_url, timeout=15)
                    pq = PyQuery(page.text)

                    pdf_element = pq('a:contains("Посмотреть содержание книги"), a:contains("Читать фрагмент")')
                    pdf_path = pdf_element.attr("href")

                    if not pdf_path:
                        log(f"ℹ️ Для книги '{book_title}' не найден PDF")
                        continue

                    # Построение полного URL PDF
                    parsed_url = urlparse(book_url)
                    pdf_link = f"{parsed_url.scheme}://{parsed_url.netloc}{pdf_path}"

                    # Проверка существования ссылки в БД
                    cursor.execute("SELECT id FROM books WHERE book_link = %s", (pdf_link,))
                    if cursor.fetchone():
                        continue

                    cursor.execute("""INSERT INTO books (book_title, book_author, book_link)
                                      VALUES (%s, %s, %s)""",
                                   (book_title, book_author, pdf_link))
                    conn.commit()

                    book_counter += 1
                    log(f"📚 Добавлена книга: {book_title} [{book_counter}/{max_books}]")

                except Exception as e:
                    conn.rollback()
                    log(f"❌ Ошибка книги '{book_title}': {e}")
                    continue

            page_num += 1
            time.sleep(1)

    except Exception as e:
        log(f"❌ Критическая ошибка в parse_book_metadata: {e}")
    finally:
        conn.close()
        log(f"✅ Первый проход завершён. Добавлено: {book_counter} книг")


# качаем пдф в БД
def download_pdfs(batch_size):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:

        cursor.execute("""SELECT id, book_title, book_link
                          FROM books
                          WHERE book_downloaded = FALSE
                              LIMIT %s""", (batch_size,))

        books = cursor.fetchall()

        # красивый прогресс бар.
        for book_id, title, link in tqdm(books, desc="⬇️ Скачивание PDF"):
            try:
                response = requests.get(link, timeout=30)
                response.raise_for_status()

                if not response.headers.get('Content-Type', '').lower().startswith('application/pdf'):
                    log(f"⚠️ Файл не является PDF: {link}")

                    cursor.execute("""UPDATE books SET book_downloaded = TRUE WHERE id = %s""", (book_id,))
                    conn.commit()
                    continue

                pdf = response.content
                # Обновление записи в БД
                cursor.execute("""UPDATE books SET book_body = %s, book_downloaded = TRUE WHERE id = %s""",(psycopg2.Binary(pdf), book_id))
                conn.commit()
                log(f"✅ PDF сохранен: {title}")

            except Exception as e:
                conn.rollback()
                log(f"❌ Ошибка скачивания '{title}': {e}")
            time.sleep(1)

    except Exception as e:
        log(f"❌ Ошибка в download_pdfs: {e}")
    finally:
        conn.close()

# Парсинг PDF с помощью GPT4All. Не работает корректно, пока решение настройки не смог найти. возможно надо брать платные версии.
# В Данном случае качал локальную версию GPT4All. но корректно она не заработала.
def parse_pdf_with_gpt4all(pdf_bytes, original_title="", original_author="", model=None):
    try:

        if model is None:
            model = GPT4All(model_name=MODEL_NAME, model_path=MODEL_PATH, device='cpu')

        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = ""
            for page in doc:
                text += page.get_text()
                if len(text) > 3000:
                    break

        prompt = (
            f"Вот текст из книги:\n\n{text[:3000]}\n\n"
            "Пожалуйста, извлеки:\n"
            "- Название книги\n"
            "- Имя автора\n"
            "- Год публикации (если есть)\n"
            "Формат: JSON с полями title, author, year"
        )

        response = model.generate(prompt=prompt, max_tokens=512)

        # Поиск JSON в ответе с помощью регулярных выражений
        try:
            match = re.search(r'\{[\s\S]*\}', response)
            if match:
                parsed = json.loads(match.group())
            else:
                parsed = {}
        except json.JSONDecodeError:
            parsed = {}

        return {
            "title": parsed.get("title", original_title),
            "author": parsed.get("author", original_author),
            "year": parsed.get("year", "")
        }

    except Exception as e:
        # Обработка ошибок и переход на резервный метод
        log(f"❌ GPT4All не смог распарсить PDF: {e}")
        return parse_pdf_metadata_locally(pdf_bytes, original_title, original_author)

# анализа данных пдф с помощью ИИ
def parse_pdfs_with_local_analyzer(batch_size):
    conn = psycopg2.connect(**DB_CONFIG)  # Подключение к БД
    cursor = conn.cursor()

    try:

        cursor.execute("""SELECT id, book_title, book_author, book_body
                          FROM books
                          WHERE book_downloaded = TRUE
                            AND book_title_parsed IS NULL
                              LIMIT %s""", (batch_size,))

        books = cursor.fetchall()

        model = GPT4All(model_name=MODEL_NAME, model_path=MODEL_PATH, device='cpu')

        for book_id, title, author, pdf in tqdm(books, desc="🧠 GPT4All парсинг PDF"):
            try:
                parsed = parse_pdf_with_gpt4all(pdf, title, author, model)

                cursor.execute("""UPDATE books
                                  SET book_title_parsed  = %s,
                                      book_author_parsed = %s,
                                      book_year_parsed   = %s
                                  WHERE id = %s""",
                               (parsed['title'], parsed['author'], parsed['year'], book_id))
                conn.commit()
                log(f"✅ Обработано: {title}")
            except Exception as e:
                conn.rollback()
                log(f"❌ Ошибка GPT4All '{title}': {e}")
            time.sleep(1)

    except Exception as e:
        log(f"❌ Ошибка в parse_pdfs_with_local_analyzer: {e}")
    finally:
        if 'model' in locals():
            del model
        conn.close()

# Резервный метод, если AI не сработает
def parse_pdf_metadata_locally(pdf_content, original_title="", original_author=""):
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        metadata = doc.metadata or {}
        title = metadata.get("title", "").strip() or original_title
        author = metadata.get("author", "").strip() or original_author
        creation_date = metadata.get("creationDate", "")
        year = creation_date[2:6] if creation_date and creation_date.startswith("D:") else ""

        return {"title": title, "author": author, "year": year}

    except Exception as e:
        # Возврат оригинальных значений при ошибке
        log(f"❌ Ошибка при локальном парсинге: {e}")
        return {"title": original_title, "author": original_author, "year": ""}


def main():

    log("\n" + "=" * 50)
    log(f"🚀 Запуск парсера: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 50 + "\n")

    try:
        init_database()
        parse_book_metadata(max_books)
        download_pdfs(batch_size=10)
        parse_pdfs_with_local_analyzer(batch_size=10)
    except Exception as e:
        log(f"🔥 Критическая ошибка в main: {e}")
    finally:
        log("\n" + "=" * 50)
        log(f"🏁 Завершение работы парсера: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 50 + "\n")


if __name__ == "__main__":
    main() #1