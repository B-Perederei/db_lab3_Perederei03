import csv
import decimal
import psycopg2

username = 'Perederei_Bohdan'
password = 'qwerty123'
database = 'VideoGamesSales'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'Video_Games_Sales_as_at_22_Dec_2016.csv'

queryDelete = '''
DELETE FROM GamePlatformRegionSales;
DELETE FROM GamePlatform;
DELETE FROM Game;
'''

queryGetGame = '''
SELECT game_id FROM Game 
WHERE game_name = %s AND game_genre = %s AND game_publisher = %s
'''

queryGetGamePlatform = '''
SELECT game_id, plat_id FROM GamePlatform
WHERE game_id = %s AND plat_id = %s 
'''

queryGetPlatform = '''
SELECT plat_id FROM Platform 
WHERE plat_id = %s
'''

queryAddGame = '''
INSERT INTO Game (game_id, game_name, game_genre, game_publisher) VALUES (%s, %s, %s, %s)
'''

queryAddPlatform = '''
INSERT INTO Platform (plat_id, plat_name) VALUES (%s, %s)
'''

queryAddGamePlatform = '''
INSERT INTO GamePlatform (game_id, plat_id, release_year) VALUES (%s, %s, %s)
'''

queryAddGamePlatformRegionSales = '''
INSERT INTO GamePlatformRegionSales (game_id, plat_id, region_id, date_counted, copies_sold)
VALUES (%s, %s, %s, %s, %s) 
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    
    cur = conn.cursor()
    cur.execute(queryDelete)
    
    
    with open(INPUT_CSV_FILE, 'r', encoding='UTF-8') as info:
        reader = csv.DictReader(info)
        
        last_game_idx = 0
        for serial_idx, row in enumerate(reader):
            try:
                # Checking if platform exists
                cur.execute(queryGetPlatform, (row['Platform'],))
                if cur.fetchone() == None:
                    # If it's not, skipping the game
                    continue

                game_idx = cur.execute(queryGetGame, (row['Name'], row['Genre'], row['Publisher']))
                game_idx = cur.fetchone()
                if game_idx == None:
                    # Adding game, if it doesn't exist
                    last_game_idx += 1
                    game_idx = last_game_idx
                    cur.execute(queryAddGame, (game_idx, row['Name'], row['Genre'], row['Publisher']))
                else:
                    game_idx = game_idx[0]
                    
                    # Checking if game is a dublicate 
                    cur.execute(queryGetGamePlatform, (game_idx, row['Platform']))
                    if cur.fetchone() != None:
                        # If it is, skipping the game
                        print(f"Dublicate found: {serial_idx}, {row['Name']}")
                        continue


                if row['Year_of_Release'] == 'N/A':
                    cur.execute(queryAddGamePlatform, (game_idx, row['Platform'], None))
                else:
                    cur.execute(queryAddGamePlatform, (game_idx, row['Platform'], row['Year_of_Release']))

                NA_sales = decimal.Decimal(row['NA_Sales'])
                EU_Sales = decimal.Decimal(row['EU_Sales'])
                JP_Sales = decimal.Decimal(row['JP_Sales'])
                Other_Sales= decimal.Decimal(row['Other_Sales'])
                date = '2016-12-22'
                cur.execute(queryAddGamePlatformRegionSales, (game_idx, row['Platform'], 'NA', date, NA_sales))
                cur.execute(queryAddGamePlatformRegionSales, (game_idx, row['Platform'], 'EU', date, EU_Sales))
                cur.execute(queryAddGamePlatformRegionSales, (game_idx, row['Platform'], 'JP', date, JP_Sales))
                cur.execute(queryAddGamePlatformRegionSales, (game_idx, row['Platform'], 'OTHER', date, Other_Sales))
            
            except (Exception) as error:
                print(f"{serial_idx}, {row['Name']}")
                raise Exception(error)
                
    conn.commit()
