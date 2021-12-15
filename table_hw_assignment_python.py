#
# Andria Gazda
# April 4, 2018
#
# Python
# SQLite Assignment

import sqlite3 as sq

def connect():
    """
    Connect to the database

    Returns conn, a connection object
    Returns curr, a pointer (cursor) to con
    """
    # Connect to the database
    conn = sq.connect("tablesdb.sqlite")
    # Create a cursor to the database
    curr = conn.cursor()
    return conn, curr

def cleandb(curr, tables):
    """
    drop the tables if they exist so they don't make errors

    Param:  curr, a cursor to an existing database connection
            tables, list of table names
    """
    cmd = "DROP TABLE IF EXISTS {}"
    for table in tables:
        drop = cmd.format(table)
        curr.execute(drop)

def create_tables(curr):
    """
    Creates the Book, Patron, Fee, and Loan table.

    Param: curr, a cursor to an existing database connection
    """
    cmd = """
    CREATE TABLE Book(
    barcode INTEGER PRIMARY KEY NOT NULL,
    title TEXT,
    author TEXT
    );
    CREATE TABLE Patron(
    card_number INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    zipcode INTEGER
    );
    CREATE TABLE Loan(
    card_id INTEGER NOT NULL,
    book_id INTEGER NOT NULL UNIQUE,
    due_date TEXT NOT NULL,
    PRIMARY KEY (card_id, book_id)
    );
    CREATE TABLE Fee(
    id INTEGER PRIMARY KEY NOT NULL,
    card_number INTEGER,
    amount CURRENCY,
    date TEXT,
    paid INTEGER
    );
    """
    print("\nCreating Tables\n")
    curr.executescript(cmd)
    #conn.commit()

def add_information(conn, curr):
    """
    Populates the book and patron tables

    Param:  conn, a connection object to a database file
            curr, a cursor to conn
    """
    # Use the insert keyword to populate the tables
    cmd = """
    INSERT INTO Book(barcode, title, author)
    VALUES (10000000, 'A', 'Abby Normal'),
    (10000001, 'B', 'J. R. R. Tolkien'),
    (10000002, 'C', 'J. K. Rowling'),
    (10000003, 'D', 'Mary Shelley'),
    (10000004, 'E', 'Frank Herbert'),
    (10000005, 'F', 'Abby Normal'),
    (10000006, 'G', 'J. R. R. Tolkien'),
    (10000007, 'H', 'J. K. Rowling'),
    (10000008, 'I', 'Mary Shelley'),
    (10000009, 'J', 'Frank Herbert');

    INSERT INTO Patron(name, zipcode)
    VALUES ('Jenna Marbles', 92703),
    ('Game Grumps', 92603),
    ('Mark Iplier', 92703),
    ('Cry', 92603),
    ('Kanye West', 92504),
    ('Oprah Winfrey', 91621),
    ('Tom Cruise', 96613),
    ('Bob Dole', 92703),
    ('Dwayne Johnson', 92603),
    ('Mel Brookes', 92804);
    """
    curr.executescript(cmd)
    conn.commit()

def add_loan_info(conn, curr):
    """
    Populates the loan table

    Param:  conn, a connection object to a database file
            curr, a cursor to conn
    """
    cmd = """
    INSERT INTO Loan(card_id, book_id, due_date)
    VALUES ((SELECT card_number FROM Patron WHERE name='Jenna Marbles'), 10000004, '2018-04-05'),
    ((SELECT card_number FROM Patron WHERE name='Game Grumps'), 10000003, '2018-04-07'),
    ((SELECT card_number FROM Patron WHERE name='Mark Iplier'), 10000002, '2018-04-12'),
    ((SELECT card_number FROM Patron WHERE name='Cry'), 10000001, '2018-04-04'),
    ((SELECT card_number FROM Patron WHERE name='Kanye West'), 10000000, '2018-04-06'),
    ((SELECT card_number FROM Patron WHERE name='Oprah Winfrey'), 10000005, '2018-04-05'),
    ((SELECT card_number FROM Patron WHERE name='Tom Cruise'), 10000007, '2018-04-07'),
    ((SELECT card_number FROM Patron WHERE name='Bob Dole'), 10000008, '2018-04-12'),
    ((SELECT card_number FROM Patron WHERE name='Dwayne Johnson'), 10000006, '2018-04-04'),
    ((SELECT card_number FROM Patron WHERE name='Mel Brookes'), 10000009, '2018-04-06');
    """
    curr.execute(cmd)
    conn.commit()

def add_fee_info(conn, curr):
    """
    Populates the fee table

    Param:  conn, a connection object to a database file
            curr, a cursor to conn
    """
    overdue = """
    SELECT card_id FROM Loan WHERE Loan.due_date < "2018-04-09";
    """
    curr.execute(overdue)
    my_fees = curr.fetchall()

    for fee in my_fees:
        cmd = """
        INSERT INTO Fee (card_number, amount, date, paid)
        VALUES ({}, 3.00, (SELECT due_date FROM Loan WHERE card_id = {}), 0);
        """.format(fee[0], fee[0])
        curr.execute(cmd)

    conn.commit()

def find_overdue(curr):
    """
    Fetches the title of overdue books and prints them.

    Param: curr, a cursor to a connection object to a database file
    """
    overdue_books = """
    SELECT Book.title
    FROM Loan JOIN Book
    ON Loan.book_id = Book.barcode
    WHERE Loan.due_date < "2018-04-10";
    """
    curr.execute(overdue_books)
    book_list = curr.fetchall()
    pretty_print_overdue(book_list)

def find_loaned_books(curr):
    """
    Fetches the title of loaned books, their author, and due date. It then prints them.

    Param: curr, a cursor to a connection object to a database file
    """
    loaned_books = """
    SELECT Book.title, Book.author, Loan.due_date
    FROM Book JOIN Loan
    ON Book.barcode = Loan.book_id
    """
    curr.execute(loaned_books)
    loan_list = curr.fetchall()
    pretty_print_loaned_books(loan_list)

def find_borrowing_patrons(curr):
    """
    Fetches the name of Patrons who have books on loan, their card number, and the book they borrowed. It then prints them.

    Param: curr, a cursor to a connection object to a database file
    """
    check_list = """
    SELECT Patron.name, Patron.card_number, Book.title
    FROM Book
    JOIN Patron ON Book.barcode = Loan.book_id
    JOIN Loan ON Patron.card_number = Loan.card_id
    """
    curr.execute(check_list)
    checked_books = curr.fetchall()
    pretty_print_borrowing_patrons(checked_books)

def find_patrons_with_fees(curr):
    """
    Fetches the name of Patrons with fees, the book they borrowed, the due date they missed, and their fee amount.
    It then prints them.

    Param: curr, a cursor to a connection object to a database file
    """
    fee_list = """
    SELECT Patron.name, book.title, Loan.due_date, Fee.amount
    FROM Loan
    JOIN Patron ON Patron.card_number = Loan.card_id
    JOIN Book ON Book.barcode = Loan.book_id
    JOIN Fee ON Fee.card_number = Loan.card_id
    """
    curr.execute(fee_list)
    my_fees = curr.fetchall()
    pretty_print_patron_fees(my_fees)

def pretty_print_patron_fees(records):
    """
    To see which patrons have fees
    Nicely prints the name of the card holder, the book they rented, the missed due date, and the current charge

    Param: records, a fetchall() from the Patron, Book, Fee, and Loan table join
    """
    print()
    print("Patrons with Fees Table:")
    print("{:15s}{:15s}{:15s}{:15s}".format("Card Holder", "Book Loaned", "Due Date", "Fee"))
    for rec in records:
        print("{:<15s}{:<15s}{:<15s}${:<d}.00".format(rec[0], rec[1], rec[2], rec[3]))
    print()

def pretty_print_borrowing_patrons(records):
    """
    To see which patrons have currently borrowed books
    Prints the name of the card holder, their card number, and the title loaned nicely

    Param: records, a fetchall() from the Patron, Book and Loan table join
    """
    print()
    print("Table of Patrons with Checked Books:")
    print("{:15s}{:15s}{:15s}".format("Card Holder", "Card Number", "Book Loaned"))
    for rec in records:
        print("{:<15s}{:<15d}{:<15s}".format(rec[0], rec[1], rec[2]))
    print()

def pretty_print_loaned_books(records):
    """
    Prints the loaned book list nicely including title, author, and due date.

    Param: records, a fetchall() from the Book and Loan table join
    """
    print()
    print("Loaned Book Table:")
    print("{:10s}{:20s}{:15s}".format("Title", "Author", "Due Date"))
    for rec in records:
        print("{:<10s}{:<20s}{:<15s}".format(rec[0], rec[1], rec[2]))
    print()

def pretty_print_overdue(records):
    """
    Prints the over due book titles nicely.

    Param: records, a fetchall() from the Loan and Book table's join.
    """
    print()
    print("Overdue Book Table:")
    print("{:15s}".format("Book Title"))
    for rec in records:
        print("{:<15s}".format(rec[0]))
    print()

def main():
    # 0. Create Tables
    tables = ["Book", "Patron", "Loan", "Fee"]
    conn, curr = connect()
    cleandb(curr, tables)
    create_tables(curr)

    # 1.  Populate the tables (any way you want) with at least 10 records in each table.
    add_information(conn, curr)
    add_loan_info(conn, curr)
    add_fee_info(conn, curr)

    # 2. Print out the title of all overdue books
    # JOIN Loan and Book table based on book id. Select book.name FROM the join
    find_overdue(curr)

    # 4.  Print out a table of the books that are currently checked out.  The table should include
    # the title, author, and due date.
    find_loaned_books(curr)

    # 5.  Print out a table of people who actually checked out books.  The table should contain
    # the person’s name, card number, and title of the book they checked out.
    # joins the book, patron and loan tables!
    find_borrowing_patrons(curr)

    # 6.  Print out the people who owe fees.  You should print the person’s name, the book title,
    # the date the book was due, and the fee amount.
    find_patrons_with_fees(curr)

if __name__ == "__main__":
    main()
