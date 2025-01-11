from flask import Flask, jsonify, request, render_template
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from typing import Dict, List
import json

# Database connection parameters
DB_PARAMS = {
    "user": "postgres.wrzghifymvmmflemjpnd",
    "password": "Achiadi123",
    "host": "aws-0-ap-southeast-1.pooler.supabase.com",
    "port": "6543",
    "dbname": "postgres"
}

app = Flask(__name__)

# Helper function to connect to the database
def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/states", methods=["GET"])
def get_states():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT state_code, state_name FROM panchayat_members;")
        states = [dict(state_code=row[0], state=row[1]) for row in cursor.fetchall()]
        return jsonify(states)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/districts/<state_code>", methods=["GET"])
def get_districts(state_code):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT district_code, district_name FROM panchayat_members WHERE state_code = %s;", (state_code,))
        districts = [dict(district_code=row[0], district=row[1]) for row in cursor.fetchall()]
        return jsonify(districts)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/taluks/<district_code>", methods=["GET"])
def get_taluks(district_code):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT block_code, block_name FROM panchayat_members WHERE district_code = %s;", (district_code,))
        taluks = [dict(taluk_code=row[0], taluk=row[1]) for row in cursor.fetchall()]
        return jsonify(taluks)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/villages/<taluk_code>", methods=["GET"])
def get_villages(taluk_code):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT gram_panchayat_code, gram_panchayat_name FROM panchayat_members WHERE block_code = %s;", (taluk_code,))
        villages = [dict(village_code=row[0], village=row[1]) for row in cursor.fetchall()]
        return jsonify(villages)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/members", methods=["GET"])
def get_members():
    try:
        village_code = request.args.get("village_code")
        connection = get_db_connection()
        cursor = connection.cursor()

        if village_code:
            query = """
SELECT elected_name AS name, designation_name, mobile_number, email_id, 
       gram_panchayat_name, block_name, district_name, state_name 
FROM panchayat_members 
WHERE gram_panchayat_code = %s;
"""
            cursor.execute(query, (village_code,))
        else:
            query = """
            SELECT name, designation_name, mobile_number, email_id, gram_panchayat_name, block_name, district_name, state_name 
            FROM panchayat_members;
            """
            cursor.execute(query)

        members = [
            dict(
                name=row[0],
                role=row[1],
                phone=row[2],
                email=row[3],
                village=row[4],
                taluk=row[5],
                district=row[6],
                state=row[7]
            ) for row in cursor.fetchall()
        ]
        return jsonify(members)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/members/add", methods=["POST"])
def add_member():
    try:
        data = request.json
        connection = get_db_connection()
        cursor = connection.cursor()

        # Check if village exists
        cursor.execute("SELECT * FROM panchayat_members WHERE gram_panchayat_code = %s LIMIT 1;", (data["village_code"],))
        village_data = cursor.fetchone()
        if not village_data:
            return jsonify({"success": False, "message": "Invalid village code."}), 400

        # Insert new member
        insert_query = """
        INSERT INTO panchayat_members (
            state_code, state_name, district_code, district_name, 
            block_code, block_name, gram_panchayat_code, gram_panchayat_name, 
            name, mobile_number, email_id, designation_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (
                village_data[1], village_data[2], village_data[3], village_data[4],
                village_data[5], village_data[6], village_data[7], village_data[8],
                data["name"], data["phone"], data["email"], data["role"]
            )
        )
        connection.commit()
        return jsonify({"success": True, "message": "Member added successfully"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

@app.route("/api/search")
def search():
    search_term = request.args.get("term", "").lower()
    if not search_term or len(search_term) < 2:
        return jsonify({"results": []})

    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        query = """
        SELECT DISTINCT state_code, state_name, district_code, district_name, block_code, block_name, gram_panchayat_code, gram_panchayat_name
        FROM panchayat_members
        WHERE LOWER(state_name) LIKE %s OR LOWER(district_name) LIKE %s 
        OR LOWER(block_name) LIKE %s OR LOWER(gram_panchayat_name) LIKE %s
        LIMIT 10;
        """
        cursor.execute(query, tuple([f"%{search_term}%"] * 4))
        results = [
            dict(
                state_code=row[0],
                state=row[1],
                district_code=row[2],
                district=row[3],
                taluk_code=row[4],
                taluk=row[5],
                village_code=row[6],
                village=row[7]
            ) for row in cursor.fetchall()
        ]
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"results": [], "error": str(e)}), 500
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
