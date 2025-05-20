ANSWR

1. trigger
   IF NEW.child_age >= 16 THEN
		SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Возраст ребенка не должен быть 16 лет или больше';
    END IF;


2. load data
   LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/testdb_table_Cities.csv"
  INTO TABLE dedmoroz_1.cities
  FIELDS TERMINATED BY ';'
  ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 ROWS;


3. SHOW VARIABLES LIKE "secure_file_priv";


4. создание пользователей:
  CREATE USER 'DedMoroz'@'localhost' IDENTIFIED BY 'Z1m@Snezhno123!';
  CREATE USER 'Snegurochka'@'localhost' IDENTIFIED BY 'Snegurochka@2025';
  CREATE USER 'Elf_logist'@'localhost' IDENTIFIED BY 'ElfLogist#25';
  
  GRANT ALL PRIVILEGES ON dedmoroz_1.* TO 'DedMoroz'@'localhost';
  
  GRANT SELECT, INSERT, UPDATE ON dedmoroz_1.letters TO 'Snegurochka'@'localhost';
  GRANT SELECT, INSERT, UPDATE ON dedmoroz_1.gifts TO 'Snegurochka'@'localhost';
  
  GRANT SELECT, INSERT, UPDATE ON dedmoroz_1.deliveries TO 'Elf_logist'@'localhost';


5. queries:
   --  Города с самыми тяжелыми погодными условиями

  SELECT c.city_name, COUNT(*) AS cold_days
  FROM weather w
  JOIN cities c ON w.city_id = c.city_id
  WHERE w.min_temperature < -10
  GROUP BY c.city_name
  ORDER BY cold_days DESC;
  
  
  --  Города с высокой загруженностью доставок
  
  SELECT c.city_name, COUNT(DISTINCT d.delivery_id) AS total_deliveries
  FROM cities c
  JOIN warehouses w ON w.city_id = c.city_id
  LEFT JOIN deliveries d ON d.warehouse_id = w.warehouse_id AND d.delivery_status = 'доставлен'
  GROUP BY c.city_name
  ORDER BY total_deliveries DESC;
  
  
  --  Определение точек срыва доставки
  
  SELECT  d.delivery_id, c.city_name, DATEDIFF(d.delivery_date, d.send_date) AS duration_days, wt.weather_date, wt.min_temperature
  FROM deliveries d
  JOIN warehouses wr ON wr.warehouse_id = d.warehouse_id
  JOIN cities c ON c.city_id = wr.city_id
  LEFT JOIN weather wt ON wt.city_id = wr.city_id AND wt.weather_date BETWEEN d.send_date AND d.delivery_date
  WHERE DATEDIFF(d.delivery_date, d.send_date) > 3
  ORDER BY duration_days DESC;
  
  
  -- Хватит ли подарков на складе для всех доставок
  
  SELECT g.gift_name, wh.warehouse_name, s.quantity AS available, COUNT(*) AS required, s.quantity - COUNT(*) AS difference
  FROM deliveries d
  JOIN warehouses wh ON wh.warehouse_id = d.warehouse_id
  JOIN stocks s ON s.warehouse_id = wh.warehouse_id
  JOIN gifts g ON s.gift_id = g.gift_id
  WHERE d.delivery_status = 'сборка'
  GROUP BY g.gift_id, wh.warehouse_name, g.gift_name, s.quantity
  ORDER BY available ASC;


6. код python:
   import mysql.connector
  import requests
  import datetime
  import json
  
  def get_cities_from_db():
      connection = mysql.connector.connect(
          host='localhost',
          user='root',
          password='password',
          database='dedmoroz_1'
      )
      cursor = connection.cursor(dictionary=True)
      cursor.execute("SELECT city_id, city_name, latitude, longitude FROM cities")
  
      cities = cursor.fetchall()
      cursor.close()
      connection.close()
      return cities
  
  def get_min_temp_for_city(lat, lon):
      url = (
          "https://archive-api.open-meteo.com/v1/archive"
          "?latitude={lat}&longitude={lon}"
          "&start_date=2024-12-01&end_date=2024-12-31"
          "&daily=temperature_2m_min&timezone=auto"
      ).format(lat=lat, lon=lon)
  
      response = requests.get(url)
      if response.status_code == 200:
          data = response.json()
          dates = data.get('daily', {}).get('time', [])
          temps = data.get('daily', {}).get('temperature_2m_min', [])
          return dict(zip(dates, temps))
      else:
          print(f"Ошибка при запросе API для координат {lat}, {lon}: {response.status_code}")
          return []
  
  def main():
      cities = get_cities_from_db()
      results = []
  
      for city in cities:
          print(f"Обработка города: {city['city_name']}")
          min_temps = get_min_temp_for_city(city['latitude'], city['longitude'])
          mounth_data = {
              'city_id': city['city_id'],
              'city_name': city['city_name'],
              'latitude': city['latitude'],
              'longitude': city['longitude'],
              'min_temperatures_december': min_temps
          }
          print(mounth_data)
          results.append(mounth_data)
  
      with open("december_2024_min_temperatures.json", "w", encoding="utf-8") as f:
          json.dump(results, f, ensure_ascii=False, indent=4)
  
      print("Данные успешно сохранены в december_2024_min_temperatures.json")
  
  if __name__ == "__main__":
      main()
  
