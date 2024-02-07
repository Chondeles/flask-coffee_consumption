import base64
from io import BytesIO
from flask import Flask, render_template, request, redirect, url_for, session, flash
from matplotlib import pyplot as plt
from dal import CoffeeDatabase  # Import the CoffeeDatabase class
import pandas as pd

app = Flask(__name__)
app.secret_key = '@#chzo71'

# Create an instance of CoffeeDatabase
# Replace these with your actual database credentials
db = CoffeeDatabase(user='root' , password='10122002', host='localhost', database='coffeedb')

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        user = db.get_user_by_email(email)  # Use the db instance

        if user and user.password == password:
            session['email'] = user.email
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid email or password. Please try again.', 'error')

    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')

        existing_user = db.get_user_by_email(email)  # Use the db instance

        if existing_user is not None:
            flash('Email already registered. Please log in.', 'error')
            return redirect(url_for('login'))

        db.create_user(email, password)  # Use the db instance

        flash('Registration successful. Please log in.', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')

@app.route('/dashboard')
def dashboard():
    if 'email' not in session:
        flash('Please log in to view the dashboard.', 'error')
        return redirect(url_for('login'))

    # Assuming db is your CoffeeDatabase instance
    coffee_data = db.get_extended_consumption_data() # Fetch coffee data from database
    return render_template('dashboard.html', email=session['email'], coffee_data=coffee_data)

@app.route('/add-coffee', methods=['GET', 'POST'])
def add_coffee():
    if 'email' not in session:
        flash('Please log in to access this page.', 'error')
        return redirect(url_for('login'))

    if request.method == 'POST':
        coffee_type = request.form.get('coffee_type')
        country_name = request.form.get('country_name')
        year = request.form.get('year')
        consumption = request.form.get('consumption')
        db.insert_full_coffee_data(coffee_type, country_name, year, consumption)
        flash('New coffee data added successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('add_coffee.html')

@app.route('/update-coffee/<int:id>', methods=['GET', 'POST'])
def update_coffee(id):
    if 'email' not in session:
        flash('Please log in to access this page.', 'error')
        return redirect(url_for('login'))

    # Fetch existing data to pre-fill the form (for GET request)
    if request.method == 'GET':
        existing_data = db.get_specific_coffee_data(id)  # Method to fetch data by ID
        if existing_data is None:
            flash('Coffee data not found.', 'error')
            return redirect(url_for('dashboard'))
        return render_template('update_coffee.html', coffee_data=existing_data)

    # Handle form submission (for POST request)
    if request.method == 'POST':
        coffee_type = request.form.get('coffee_type')
        country_name = request.form.get('country_name')
        year = request.form.get('year')
        consumption = request.form.get('consumption')
        db.update_coffee_data(id, coffee_type, country_name, year, consumption)
        flash('Coffee data updated successfully!', 'success')
        return redirect(url_for('dashboard'))

    return redirect(url_for('dashboard'))

@app.route('/delete-coffee/<int:id>')
def delete_coffee(id):
    if 'email' not in session:
        flash('Please log in to access this page.', 'error')
        return redirect(url_for('login'))
   
    print(f"Attempting to delete coffee data with ID: {id}")  # Debugging print

    db.delete_coffee_data(id)
    flash('Coffee data deleted successfully!', 'success')
    return redirect(url_for('dashboard'))

@app.route('/logout')
def logout():
    session.pop('email', None)
    return redirect(url_for('login'))

@app.route('/chart')
def chart():
    # Create a figure and axis
    fig, ax = plt.subplots()

    # Plot the DataFrame data
    df = pd.read_csv('C:/Users/lenovo/Desktop/ESISA Workspace/3eme Année/S5/Python/Python_Flask/Coffee_domestic_consumption.csv', na_values=',')
    df_cleaned = df.loc[df['Total_domestic_consumption'] != 0]
    df_cleaned[['1990/91', '2019/20']].plot(ax=ax)

    # Customize the plot
    plt.title('1990-2019')
    plt.legend()

    # Save the plot to BytesIO buffer
    img_buf = BytesIO()
    fig.savefig(img_buf, format='png')
    img_buf.seek(0)

    # Encode the image buffer in base64
    img_base64 = base64.b64encode(img_buf.read()).decode('utf-8')

    # Render the HTML template with the base64 encoded image
    return render_template('visu.html', image=img_base64)

if __name__ =='__main__':
    app.run(debug=True)
