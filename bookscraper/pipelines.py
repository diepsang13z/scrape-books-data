# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import mysql.connector

from itemadapter import ItemAdapter

from .settings import (
    DB_MYSQL_HOST,
    DB_MYSQL_PORT,
    DB_MYSQL_PASSWORD,
    DB_MYSQL_DATABASE,
)


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()

        # Category & Product Type -> Switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # Price -> Convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        # Availability -> Extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        # Reviews -> Convert string to number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        # Stars -> Convert text to number
        stars_value_dict = {
            'zero': 0,
            'one': 1,
            'two': 2,
            'three': 3,
            'four': 4,
            'five': 5,
        }
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        adapter['stars'] = stars_value_dict[stars_text_value]

        return item


class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host=DB_MYSQL_HOST,
            user=DB_MYSQL_PORT,
            password=DB_MYSQL_PASSWORD,
            database=DB_MYSQL_DATABASE,
        )

        # Create cursor, used to execute commands
        self.cur = self.conn.cursor()

        # Create books table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id INT NOT NULL AUTO_INCREMENT,
            title VARCHAR(255),
            price DECIMAL,
            category VARCHAR(255),
            description TEXT,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            url VARCHAR(2083),
            PRIMARY KEY(id)
        )
        """)

    def process_item(self, item, spider):
        insert_query = """
        INSERT INTO books (
            title,
            price,
            category,
            description,
            upc,
            product_type,
            price_excl_tax,
            price_incl_tax,
            tax,
            availability,
            num_reviews,
            stars,
            url
        ) values (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """
        insert_value = (
            item['title'],
            item['price'],
            item['category'],
            str(item['description']),
            item['upc'],
            item['product_type'],
            item['price_excl_tax'],
            item['price_incl_tax'],
            item['tax'],
            item['availability'],
            item['num_reviews'],
            item['stars'],
            item['url']
        )

        # Execute insert data into database
        self.cur.execute(insert_query, insert_value)
        self.conn.commit()

        return item

    def close_spider(self, spider):
        # Close curser & connection
        self.cur.close()
        self.conn.close()
