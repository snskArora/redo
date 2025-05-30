from base import BaseModel
from db import DatabaseConnection


class User(BaseModel):
    _table_name = "users"
    _primary_key = "id"


class Post(BaseModel):
    _table_name = "posts"
    _primary_key = "id"


if __name__ == "__main__":
    db = DatabaseConnection(
        host="localhost",
        port=5432,
        database="ormtest",
        user="postgres",
        password="SudoPass",
    )

    db1 = DatabaseConnection(
        host="localhost",
        port=5432,
        database="ormtest_m1",
        user="postgres",
        password="SudoPass",
    )

    db2 = DatabaseConnection(
        host="localhost",
        port=5432,
        database="ormtest_m2",
        user="postgres",
        password="SudoPass",
    )

    db3 = DatabaseConnection(
        host="localhost",
        port=5432,
        database="ormtest_m3",
        user="postgres",
        password="SudoPass",
    )

    BaseModel.set_database([db, db1, db2, db3])

    try:
        print("=== Testing Database Connection ===")
        conn = db.connection
        with conn.cursor() as cursor:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            print(f"PostgreSQL version: {version[0]}")

        print("\n=== Creating Tables ===")
        # "email": "VARCHAR(255) UNIQUE NOT NULL",
        User.create_table(
            {
                "name": "VARCHAR(100) NOT NULL",
                "email": "VARCHAR(255) NOT NULL",
                "age": "INTEGER",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }
        )

        Post.create_table(
            {
                "title": "VARCHAR(200) NOT NULL",
                "content": "TEXT",
                "user_id": "INTEGER REFERENCES users(id)",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }
        )

        print("\n=== Creating Records ===")
        user1 = User.create(name="John Doe", email="john@example.com", age=30)
        print(f"Created user: {user1}")

        user2 = User.create(name="Jane Smith", email="jane@example.com", age=25)
        print(f"Created user: {user2}")

        post1 = Post.create(
            title="My First Post",
            content="This is my first blog post!",
            user_id=user1.id,
        )
        print(f"Created post: {post1}")

        print("\n=== Reading Records ===")
        found_user = User.find_by_id(user1.id)
        print(f"Found user: {found_user}")

        all_users = User.find_all()
        print(f"All users: {all_users}")

        adult_users = User.find_all("age >= %s", (25,))
        print(f"Adult users: {adult_users}")

        print("\n=== Updating Records ===")
        user1.age = 31
        user1.save()
        print(f"Updated user: {user1}")

        print("\n=== Testing Delete Operations ===")
        user_to_delete = User.create(name="Temp User", email="temp@example.com", age=20)
        print(f"Created temp user: {user_to_delete}")

        deleted = user_to_delete.delete()
        print(f"Temp user deleted: {deleted}")

        deleted_user = User.find_by_id(user_to_delete.id)
        print(f"Trying to find deleted user: {deleted_user}")

        print(f"\n=== Final Record Count ===")
        final_users = User.find_all()
        final_posts = Post.find_all()
        print(f"Total users in database: {len(final_users)}")
        print(f"Total posts in database: {len(final_posts)}")

        print("\n=== Script completed successfully! ===")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()

    finally:
        db.disconnect()
