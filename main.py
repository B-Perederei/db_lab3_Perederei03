import psycopg2
import matplotlib.pyplot as plt

username = 'Perederei_Bohdan'
password = 'qwerty123'
database = 'VideoGamesSales'
host = 'localhost'
port = '5432'

queryCreatingView1 = '''
CREATE VIEW CopiesSoldByPlatform AS
SELECT platform.plat_id, SUM(copies_sold) AS copies_sold
FROM (GamePlatformRegionSales JOIN platform ON GamePlatformRegionSales.plat_id = platform.plat_id) 
GROUP BY platform.plat_id
ORDER BY platform.plat_id;
'''

queryCreatingView2 = '''
CREATE VIEW RealisesOnEachPlatform AS
SELECT plat_name, COUNT(game_id) AS games_realised
FROM (GamePlatform JOIN platform ON GamePlatform.plat_id = platform.plat_id) 
GROUP BY plat_name
ORDER BY plat_name;
'''

queryCreatingView3 = '''
CREATE VIEW RealisesEachYear AS 
SELECT release_year, COUNT(game_id) AS games_realised FROM GamePlatform 
GROUP BY release_year
ORDER BY release_year;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)

with conn:
    platform = []
    copies_sold = []
    
    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS CopiesSoldByPlatform')
    cur.execute(queryCreatingView1)
    cur.execute('SELECT * FROM CopiesSoldByPlatform')
    
    for row in cur:
        platform.append(row[0])
        copies_sold.append(row[1])
    
    x_range = range(len(platform))
 
    bar_ax.bar(x_range, copies_sold, label='Millions of copeis sold')
    bar_ax.set_title('Total number of sold copies grouped by platform (in millions)')
    bar_ax.set_xlabel('Platform')
    bar_ax.set_ylabel('Millions of copeis')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(platform)
    

with conn:
    platform = []
    games_released = []
    
    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS RealisesOnEachPlatform')
    cur.execute(queryCreatingView2)
    cur.execute('SELECT * FROM RealisesOnEachPlatform')
    
    for row in cur:
        platform.append(row[0])
        games_released.append(row[1])
    
    pie_ax.pie(games_released, labels=platform, autopct='%2.2f%%')
    pie_ax.set_title("Number of games released on each platform")

with conn:
    year = []
    games_released = []
    
    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS RealisesEachYear')
    cur.execute(queryCreatingView3)
    cur.execute('SELECT * FROM RealisesEachYear')
    
    for row in cur:
        year.append(row[0])
        games_released.append(row[1])
    
    graph_ax.plot(year, games_released, marker='o')
    graph_ax.set_xlabel('Year of release')
    graph_ax.set_ylabel('Number of games released')
    graph_ax.set_title('Number of releases each year')

    for y, g_r in zip(year, games_released):
        graph_ax.annotate(g_r, xy=(y, g_r), xytext=(7, 2), textcoords='offset points') 
    

mng = plt.get_current_fig_manager()
mng.resize(1400, 800)
      
plt.subplots_adjust(left=0.08,
                    bottom=0.1,
                    right=0.95,
                    top=0.9,
                    wspace=0.5,
                    hspace=0.5)  
                                        
plt.show()