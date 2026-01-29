import ee

# Trigger the authentication flow
ee.Authenticate()

# Initialize the library with your specific Cloud Project ID
# Replace 'your-project-id' with the ID from your Google Cloud Console
ee.Initialize(project='absolute-cache-478407-p5')