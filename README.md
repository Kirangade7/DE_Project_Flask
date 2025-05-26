# Hosting a Flask Application on EC2 with Gunicorn and Nginx

This guide will walk you through the process of setting up a Flask application on an EC2 instance, using Gunicorn as the WSGI server and Nginx as a reverse proxy.

Certainly! Let's delve into each step with a bit more detail:

# Step -1 Install Python Virtualenv

 sudo apt-get update
 sudo apt-get install python3-venv

This step ensures that your EC2 instance has the necessary tools to create and manage virtual environments for Python.

# Step - 2: Set up the Virtual Environment

  mkdir project
  cd project
  python3 -m venv venv
  source venv/bin/activate

Here, you create a project directory and set up a virtual environment within it. Activating the virtual environment isolates your project's dependencies, preventing conflicts with other python projects on the same machine.

# Step 3: Install Flask

  pip install flask

This installs the Flask framework within your virtual environment, allowing you to develop web applications using Python.

# Step 4: Create a simple Flask API (Clone Github repo)

  git clone <link>

You clone your Flask application's code from a Github repository. This step assumes you have your Flask application hosted on a Git repository.

  python run.py
  
Verifying that the application works ensures that your Flask API is set up correctly.

# Step 5: Install Gunicorn

  pip install gunicorn

Gunicorn or Green Unicorn is a WSGI server for running Flask applications. Installing it is a crucial step in deploying a production-ready Flask app.

   gunicorn -b 0.0.0.0:8000 app:app

You run Gunicorn, binding it to address 0.0.0.0:8000, and specifying the entry point to your Flask application (app:app)

# Step 6: Use systemd to Manage Gunicorn

  You create a systemd unit file to manage the Gunicorn process as a service.

  sudo nano /etc/systemd/system/project.service

The unit file specifies the user, working directory, and command to start Gunicorn as a service.

  [Unit]
  Description=Gunicorn instance for a de-project
  After=network.target

  [Service]
  User=ubuntu
  Group=www-data
  WorkingDirectory=/home/ubuntu/project
  ExecStart=/home/ubuntu/project/venv/bin/gunicorn -b localhost:8000 app:app
  Restart=always

  [Install]
  WantedBy=multi-user.target

# After creating the unit file, you enable and start the Gunicorn service.

  sudo systemct1 daemon-reload
  sudo systemct1 start project
  sudo systemct1 enable project

# Step 7: Run Nginx Webserver

  sudo apt-get install nginx

# Nginx is a web server that will act as a reverse proxy for your Flask application, forwarding requests to Gunicorn.

  sudo systemct1 start nginx
  sudo systemct1 enable nginx

# Starting and enabling Nginx ensures that it runs automatically after a system reboot.

  sudo nano /etc/nginx/sites-available/default

# You configure Nginx by editing its default configuration file, specifying the upstream server(Gunicorn) and the location for proxying requests.

  upstream flask_project {
      server 127.0.0.1:5000;
  }

  location  / {
      proxy_pass http://flask_project;
  }

# After editing the configuration, you restart Nginx to apply the changes.

  sudo systemct1 restart nginx

# Visiting your EC2 instance's public IP address in a browser confirms that your Flask application is now accessible through Nginx, completing the deployment process.

# Create   Lambda Layers

 To create layers for your lambda handler, you need to package the required dependencies and libraries into a zip file and then upload it as a layer in the AWS Lambda 
  console or through the AWS CLI. Here are the steps to create layers for your lambda handler:
   1. Create a directory for the Layer: Create a direcctory to organize the files that will be included in your layer.

      mkdir lambda_layer
      cd lambda_layer
   2.Install dependencies: Install the required dependencies(e.g. pandas, boto3, etc) into the directory.  You can use a virtual environment to avoid conflicts with your 
      system's libraries.

      pip install pandas -t.
      pip install boto3 -t.

      the -t . flag tells pip to install the packages in the current directory.

  3. Remove unnecessary Files: Remove any unnecessary files, such as test files or documentation, to keep the layer size smaller.

  4. Create a Zip File: Create a zip file containing the contents of the directory.

      zip -r lambda_layer.zip .

  5. Upload as a layer: Open the AWS Lambda Console, navigate to the Layers section, and click "Create Layer".
       Provide a name, description, and upload the zip file.

  6. Attach the Layer to your Lambda Function: Open your Lambda function in the AWS Lambda Console. In the "Designer" section, click "Layers" and then "Add a layer."             Select the layer you created.

By following these steps, you can create and attach layers to your Lambda function, allowing you to manage dependencies separately and keep your Lambda deployment package smaller.

    
    
