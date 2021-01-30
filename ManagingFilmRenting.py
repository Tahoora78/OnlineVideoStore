import datetime
import mysql.connector
import now as now


class FilmRenting:
    def __init__(self):
        self.cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="filmRenting"
        )
        self.last_film_id = 1
        self.last_shop_id = 1
        self.cursor = self.cnx.cursor(buffered=True)

        self.making_film_table()
        self.making_shops_table()
        self.making_film_actors_table()
        self.making_film_shops_table()
        self.making_customer_table()
        self.making_rent_film_table()
        self.cnx.commit()

    def making_film_table(self):
        self.cursor.execute(
            'CREATE TABLE Films(film_id INT ,language VARCHAR(50),title VARCHAR(255),release_year INT(4),length INT(2),rating INT(2),film_category VARCHAR(255),RENTING_PRICE FLOAT(10),video VARCHAR(255),PRIMARY KEY(film_id));')

    def making_shops_table(self):
        self.cursor.execute(
            'CREATE TABLE Shops(shop_id INT,name VARCHAR(255),address VARCHAR(255),charge FLOAT(10),password VARCHAR(5),PRIMARY KEY(shop_id));'
        )

    def making_film_actors_table(self):
        self.cursor.execute(
            'CREATE TABLE Actors(film_id INT,actor_name VARCHAR(255),PRIMARY KEY(actor_name,film_id),FOREIGN KEY (film_id) REFERENCES Films(film_id));'
        )

    def making_film_shops_table(self):
        self.cursor.execute(
            'CREATE TABLE Film_shops(shop_id INT,film_id INT,PRIMARY KEY(shop_id,film_id), FOREIGN KEY (film_id) REFERENCES Films(film_id), FOREIGN KEY (shop_id) REFERENCES Shops(shop_id));'
        )

    def making_customer_table(self):
        self.cursor.execute(
            'CREATE TABLE Customers(customer_name VARCHAR(255),address VARCHAR(255),phone_number VARCHAR(11),charge FLOAT(20),password VARCHAR(5),PRIMARY KEY(customer_name));'
        )

    def making_rent_film_table(self):
        self.cursor.execute(
            'CREATE TABLE Rent_film(customer_name VARCHAR(255),film_id INT,receive_date DATETIME,return_date DATETIME, returned BOOLEAN,PRIMARY KEY(customer_name,film_id),FOREIGN KEY(customer_name) REFERENCES Customers(customer_name),FOREIGN KEY (film_id) REFERENCES Films(film_id));'
        )

    def adding_film(self, title, language, year, length, rating, category, renting_price, video, shop_name):
        self.cursor.execute(
            "SELECT film_id FROM Films WHERE title = '%s' and language='%s'" % (title, language)
        )
        same_film = [j for i in self.cursor.fetchall() for j in i]
        if not same_film:
            self.cursor.execute(
                'SELECT film_id FROM Films ORDER BY film_id DESC LIMIT 1;'
            )
            film_id = [j for i in self.cursor.fetchall() for j in i]
            print("film_id",film_id!=[])
            if film_id == []:
                film_id = 1
            if film_id !=[] and film_id != 1:
                film_id = film_id[0] + 1
            print("INSERT INTO Films VALUES ( %i,'%s','%s',%i,%i,%i,'%s',%i,'%s')" %
                (film_id, language, title, year, length, rating, category, renting_price, video))
            self.cursor.execute(
                "INSERT INTO Films VALUES ( %i,'%s','%s',%i,%i,%i,'%s',%i,'%s')" %
                (film_id, language, title, year, length, rating, category, renting_price, video)
            )
            #finding the shop_id of shop
            self.cursor.execute(
                "SELECT shop_id FROM Shops WHERE name='%s';" % shop_name
            )
            shop_id = [j for i in self.cursor.fetchall() for j in i][0]

            self.cursor.execute(" drop trigger if exists update_film_shops")
            query = "CREATE TRIGGER update_film_shops BEFORE INSERT ON Films FOR EACH ROW BEGIN INSERT INTO Film_shops(shop_id, film_id) VALUES (%i,%i);END" % (shop_id, film_id)
            self.cursor.execute(query)

            print("shop_id , silm_id",shop_id,film_id)
            #self.cursor.execute(
            #    'INSERT INTO Film_shops VALUES(%i,%i)' % (shop_id, film_id)
            #)


            self.cnx.commit()
        else:
            print("There is tha same film with this information!")

    def adding_actors(self, actor_name, film_name):
        try:
            self.cursor.execute(
                "SELECT film_id FROM Films WHERE title='%s'" % film_name
            )
            film_id = int([j for i in self.cursor.fetchall() for j in i][0])

            self.cursor.execute(
                "INSERT INTO Actors VALUES(%i,'%s')" % (film_id, actor_name)
            )
            self.cnx.commit()
        except:
            print("This Actor (%s) has been saved for this movie (%s)" % (actor_name, film_name))

    def adding_shop(self, address, name, password):
        self.cursor.execute("SELECT shop_id  FROM Shops WHERE name = '%s';" % name)
        same_shop = self.cursor.fetchall()
        if not same_shop:
            self.cursor.execute(
                "SELECT shop_id  FROM Shops ORDER BY shop_ID DESC LIMIT 1;"
            )
            shop_id = [j for i in self.cursor.fetchall() for j in i]
            shop_id = 1 if shop_id==[] else (shop_id[0]+1)
            self.cursor.execute(
                "INSERT INTO Shops VALUES(%i,'%s','%s',%i,'%s')" % (shop_id, name, address, 0, password)
            )
            self.cnx.commit()
            return True, "your shop is added to the site"
        else:
            return False, "There is another shop with this name and address!"

    def total_cost_of_all_movie(self):
        self.cursor.execute(
            'SELECT sum(RENTING_PRICE) AS total_price FROM Films;'
        )
        print("total price is: ", [j for i in self.cursor.fetchall() for j in i][0])

    def number_of_film_of_each_language(self):
        self.cursor.execute(
            'SELECT COUNT(film_id) AS film_number, language FROM Films GROUP BY film_id;'
        )
        print([j for i in self.cursor.fetchall() for j in i])

    def showing_rented_film_for_shop(self, shop_name):
        self.cursor.execute(
            "SELECT shop_id FROM Shops WHERE name='%s'" % shop_name
        )
        shop_id = [j for i in self.cursor.fetchall() for j in i][0]
        self.cursor.execute(
            "SELECT films FROM (SELECT Rent_film.film_id AS films, Film_shops.shop_id As shops FROM Rent_film LEFT JOIN Film_shops on Rent_film.film_id = Film_shops.film_id) AS t WHERE t.shops=%i" % shop_id
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for film in films:
            self.cursor.execute(
                "SELECT * FROM Films WHERE film_id=%i" % film
            )
            for m in [j for i in self.cursor.fetchall() for j in i]:
                print(m, end=' ')
            print()

    def showing_shop_charge(self, name):
        self.cursor.execute(
            "SELECT charge FROM Shops WHERE name='%s'" % name
        )
        print(name + "has", [j for i in self.cursor.fetchall() for j in i][0])

    def showing_film_genre(self, genre):
        self.cursor.execute(
            "SELECT * FROM Films WHERE film_category='%s'" % genre
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for m in range(len(films)):
            print(films[m], end=' ')
            if m%9==0:
                print()

    def showing_actor(self):
        self.cursor.execute(
            "SELECT actor_name FROM Actors"
        )
        actors = [j for i in self.cursor.fetchall() for j in i]
        for act in actors:
            print(act, end=' ')

    def showing_film_actor(self, actor_name):
        self.cursor.execute(
            "SELECT film_id FROM Actors WHERE actor_name='%s'" % actor_name
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for film in films:
            self.cursor.execute(
                "SELECT * FROM Films WHERE film_id=%i" % film
            )
            for i in [j for i in self.cursor.fetchall() for j in i]:
                print(i, end=' ')
            print()

    def showing_film_language(self, language):
        self.cursor.execute(
            "SELECT * FROM Films WHERE language='%s'" % language
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for m in range(len(films)):
            print(films[m], end=' ')
            if m%9==0:
                print()

    def showing_film_year(self, year):
        self.cursor.execute(
            'SELECT * FROM Films WHERE release_year=%i' % year
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for m in range(len(films)):
            print(films[m], end=' ')
            if m%9==0:
                print()

    def showing_shop_name(self):
        self.cursor.execute(
            "SELECT name FROM Shops"
        )
        names = [j for i in self.cursor.fetchall() for j in i][0]
        for i in names:
            print(i, end=' ')

    def showing_shop_film(self, name):
        self.cursor.execute(
            "SELECT shop_id FROM Shops WHERE name='%s'" % name
        )
        shop_id = [j for i in self.cursor.fetchall() for j in i][0]
        self.cursor.execute(
            'SELECT film_id FROM Film_shops WHERE shop_id=%i' % shop_id
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for film in films:
            self.cursor.execute(
                'SELECT * FROM Films WHERE film_id = %i' % film
            )
            for i in [j for i in self.cursor.fetchall() for j in i]:
                print(i, end=' ')
            print()

    def charging_shop(self, shop_id, amount):
        self.cursor.execute(
            "UPDATE Shops SET charge = charge + %i WHERE shop_id=%i" % (amount, shop_id)
        )
        self.cnx.commit()

    def showing_rented_film_for_customer_renting(self, name):
        self.cursor.execute(
            "SELECT film_id FROM Rent_film WHERE customer_name='%s' and returned = %s" % (name, int(False))
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for film in films:
            self.cursor.execute(
                "SELECT * FROM Films WHERE film_id=%i" % film
            )
            for i in [j for i in self.cursor.fetchall() for j in i]:
                print(i, end=' ')
            print()

    def showing_rented_film_shop_rented(self, name):
        self.cursor.execute(
            'SELECT Rent_film.film_id,Film_shops.film_id,Film_shops.shop_id FROM Rent_film,Film_shops WHERE Rent_film.film_id,Film_shops.film_id and Film_shops.shop_id = %i' %film_id
        )

    def showing_customer_charge(self, name):
        self.cursor.execute(
            "SELECT charge FROM Customers WHERE customer_name= '%s'" % name
        )
        print([j for i in self.cursor.fetchall() for j in i][0])

    def showing_rented_film_for_customer_rented(self, name):
        self.cursor.execute(
            "SELECT film_id FROM Rent_film WHERE customer_name='%s' and returned = %s" % (name, int(True))
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for film in films:
            self.cursor.execute(
                "SELECT * FROM Films WHERE film_id=%i" % film
            )
            for i in [j for i in self.cursor.fetchall() for j in i]:
                print(i, end=' ')

    def giving_film(self, name, film_name):
        self.cursor.execute(
            "SELECT film_id,renting_price FROM Films WHERE title = '%s'" % film_name
        )
        film_id = [j for i in self.cursor.fetchall() for j in i][0]
        now = datetime.datetime.utcnow()
        self.cursor.execute(
            "UPDATE Rent_film SET returned = %s ,receive_date = '%s' WHERE customer_name='%s' and film_id=%i;" % (int(True), now.strftime('%Y-%m-%d %H:%M:%S'), name, film_id)
        )
        self.cursor.execute(
            "SELECT film_id,renting_price FROM Films WHERE title = '%s'" % film_name
        )
        film_name = [j for i in self.cursor.fetchall() for j in i]
        self.cursor.execute(
            "select datediff(Rent_film.receive_date,Rent_film.return_date) as data_diff from Rent_film where customer_name='%s' and film_id=%i" % (name, film_name[0])
        )
        dates = [j for i in self.cursor.fetchall() for j in i][0]
        dates = 3
        self.charging_customers(name, (-1*int(dates) * film_name[1]))
        self.cursor.execute(
            "SELECT shop_id FROM Film_shops WHERE film_id=%i" % film_name[0]
        )
        shop_id = [j for i in self.cursor.fetchall() for j in i][0]
        self.cursor.execute(" drop trigger if exists update_shop_charge")
        query = "CREATE TRIGGER update_shop_charge AFTER UPDATE ON Rent_film FOR EACH ROW BEGIN update Shops set charge = charge + %i WHERE shop_id = %i;END" % ((film_name[1]*int(dates)*0.1), shop_id)
        self.cursor.execute(query)
        #self.charging_shop(shop_id, (film_name[1]*int(dates)*0.1))
        self.cnx.commit()

    def showing_movie_name(self):
        self.cursor.execute(
            'SELECT * FROM Films'
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        for m in range(len(films)):
            print(films[m], end=' ')
            if m % 9 == 0:
                print()

    def renting_video(self, name, film_name):
        self.cursor.execute(
            "SELECT film_id,renting_price FROM Films WHERE title = '%s'" % film_name
        )
        film_name = [j for i in self.cursor.fetchall() for j in i]
        if film_name:
            self.cursor.execute(
                "SELECT returned FROM Rent_film WHERE film_id = %i" % film_name[0]
            )
            rented = [j for i in self.cursor.fetchall() for j in i]
            if rented == [] or rented[0]==1:
                self.cursor.execute(
                    "SELECT charge FROM Customers WHERE customer_name = '%s'" % name
                )
                charge = [j for i in self.cursor.fetchall() for j in i][0]
                now = datetime.datetime.utcnow()
                if int(charge) >= film_name[1]:
                    self.cursor.execute(
                        "INSERT INTO Rent_film VALUES('%s',%i,'%s','%s',%s)" % (name, film_name[0], now.strftime('%Y-%m-%d %H:%M:%S'), now.strftime('%Y-%m-%d %H:%M:%S'), int(False))
                    )
                else:
                    print("Dear %s your account doesn't have enough charge, please charge it first" % name)
            else:
                print("We have this film but someone else has rented it")
        else:
                print("We don't have any film with this name (%s)" % film_name)
        self.cnx.commit()

    def adding_customer(self, name, address, phone_number, password):
        try:
            self.cursor.execute(
                "INSERT INTO Customers VALUES ('%s','%s','%s',%i,'%s')" % (name, address, phone_number, 0, password)
            )
            self.cnx.commit()
            return True, "Now you can use the site"
        except:
            return False, "There is another customer with this name!"

    def charging_customers(self, name, amount):
        self.cursor.execute(
            "UPDATE Customers SET charge = charge + %i WHERE customer_name='%s'" % (int(amount), name)
        )
        self.cnx.commit()

    def showing_movies_and_number_of_customers_that_have_rent_them(self):
        self.cursor.execute(
            'SELECT film_id ,count(customer_name) AS number FROM Rent_film GROUP BY CUBE(customer_name) order by number;'
        )
        print([j for i in self.cursor.fetchall() for j in i])

    def showing_added_actor(self, shop_name):
        self.cursor.execute(
            "SELECT shop_id FROM Shops WHERE name='%s'" % shop_name
        )
        shop_id = [j for i in self.cursor.fetchall() for j in i][0]

        self.cursor.execute(
            "SELECT Film_shops.film_id AS films, Actors.actor_name As name FROM Film_shops LEFT JOIN Actors on Film_shops.film_id = Actors.film_id where Film_shops.shop_id = %i and Actors.actor_name is not NULL" % shop_id
        )
        films = [j for i in self.cursor.fetchall() for j in i]
        print(films)

    def show_customers(self, shop_name):
        self.cursor.execute(
            "SELECT shop_id FROM Shops WHERE name='%s'" % shop_name
        )
        shop_id = [j for i in self.cursor.fetchall() for j in i][0]

        self.cursor.execute(
            "SELECT customers FROM (SELECT Rent_film.customer_name AS customers, Film_shops.shop_id As shops FROM Rent_film LEFT JOIN Film_shops on Rent_film.film_id = Film_shops.film_id) AS t WHERE t.shops=%i" % shop_id
        )
        customers = [j for i in self.cursor.fetchall() for j in i]
        for customer in customers:
            self.cursor.execute(
                "SELECT * FROM Customers WHERE customer_name='%s'" % customer
            )
            for m in [j for i in self.cursor.fetchall() for j in i]:
                print(m, end=' ')
            print()

    def check_customer_password(self, name, password):
        self.cursor.execute(
            "SELECT * FROM Customers WHERE customer_name = '%s' and password='%s'" % (name, password)
        )
        authentication = [j for i in self.cursor.fetchall() for j in i]
        if authentication:
            return True, "Welcome. Select what you want to do"
        else:
            return False, "The password isn't correct , sign up pr try more"

    def check_shop_password(self, name, password):
        self.cursor.execute(
            "SELECT * FROM Shops WHERE name = '%s' and password='%s'" % (name, password)
        )
        authentication = [j for i in self.cursor.fetchall() for j in i]
        if authentication:
            return True, "Welcome. Select what you want to do"
        else:
            return False, "The password isn't correct , sign up pr try more"


which, state = input("Please enter 1 for shop and 2 for customer, then type 1 for login and 2 for sign_up").split(',')
which = int(which)
state = int(state)
film_renting = FilmRenting()
name = ""
authentication = False
sentence = ""
change = 0
if which == 1:
    while not authentication:
        if state == 1:
            name, password = input("Enter name and password to login").split(',')
            authentication, sentence = film_renting.check_shop_password(name, password)
        else:
            (name, address, password) = input("Enter the name of the shop and address and password").split(',')
            authentication, sentence = film_renting.adding_shop(address, name, password)
        print(sentence)
        if not authentication:
            state = input("Enter 1 for login and 2 for sign_up")
    work = int(input("Works that you can do as a shop are\n:"
                 " 1)adding film 2)seeing added film 3)seeing rented films 4)seeing output 5)adding movie actor's\n"
                     "6)showing_customer 7)showing_added actor"))
    while work != 0:
        print('------------------------------------------------------------------------------------------------')
        if work == 1:
            (title, language, year, length, rating, category, renting_price, video) = \
                input("Enter the name,language,year,length,rating,category,tenting price and the url of video").split(',')
            film_renting.adding_film(title, language, int(year), int(length), int(rating), category, int(renting_price), video, name)
        if work == 2:
            film_renting.showing_shop_film(name)
        if work == 3:
            film_renting.showing_rented_film_for_shop(name)
        if work == 4:
            film_renting.showing_shop_charge(name)
        if work == 5:
            (actor_name, film_name) = input("Enter the name of the actor and name of the film").split(',')
            film_renting.adding_actors(actor_name, film_name)
        if work == 6:
            film_renting.show_customers(name)
        if work == 7:
            film_renting.showing_added_actor(name)
        print('--------------------------------------------------------------------------------------')
        work = int(input("Works that you can do as a shop are\n:"
                     " 1)adding film 2)seeing added film 3)seeing rented films 4)seeing output 5)adding movie actor's\n"
                         "6)showing customer 7)showing added actor"))
else:
    while not authentication:
        if state == 1:
            name, password = input("Enter name and password to login").split(',')
            authentication, sentence = film_renting.check_customer_password(name, password)
        else:
            (name, address, phone_number, password) = input("Enter your name,address,phone number and password ").split(',')
            authentication, sentence = film_renting.adding_customer(name, address, phone_number, password)
        print(sentence)
        if not authentication:
            state = input("Enter 1 for login or 2 for sign_up")
    work = int(input("Works that you can do as a customer are:\n"
            "1)renting a movie 2)see your rented movie 3)see movies that you are renting 4)check the charge\n"
            " 5)charging your account 6)giving back the movie 7)showing movie based on shop\n"
            "8)... category 9)...language 10)...year 11)actor 12)showing shops 13)showing all actors 14)showing_movie_name"))
    while work != 0:
        print("------------------------------------------------------------------------------------------")
        if work == 1:
            film_name = input("Enter the name of the film that you want to rent")
            film_renting.renting_video(name, film_name)
        if work == 2:
            film_renting.showing_rented_film_for_customer_rented(name)
        if work == 3:
            film_renting.showing_rented_film_for_customer_renting(name)
        if work == 4:
            film_renting.showing_customer_charge(name)
        if work == 5:
            amount = input("Enter how much you want to charge your account")
            film_renting.charging_customers(name, amount)
        if work == 6:
            film_name = input("Enter the name of the movie that you want to give back")
            film_renting.giving_film(name, film_name)
        if work == 7:
            shop_name = input("Enter the shop name")
            print("movies based on shop are:")
            film_renting.showing_shop_film(shop_name)
        if work == 8:
            category = input("Enter the your category")
            film_renting.showing_film_genre(category)
        if work == 9:
            language = input("Enter your language")
            film_renting.showing_film_language(language)
        if work == 10:
            year = int(input("Enter the year"))
            film_renting.showing_film_year(year)
        if work == 11:
            actor = input("Enter the name of actor")
            film_renting.showing_film_actor(actor)
        if work == 12:
            film_renting.showing_shop_name()
        if work == 13:
            film_renting.showing_actor()
        if work == 14:
            film_renting.showing_movie_name()
        print("------------------------------------------------------------------------------------------------------")
        work = int(input(
            "Works that you can do as a customer are:\n"
                    "1)renting a movie 2)see your rented movie 3)see movies that you are renting 4)check the charge\n"
                    " 5)charging your account 6)giving back the movie 7)showing movie based on shop\n"
                    "8)... category 9)...language 10)...year 11)actor 12)showing shops 13)showing all actors 14)showing_movie_name"))


