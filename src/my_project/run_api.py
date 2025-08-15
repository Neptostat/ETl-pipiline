
# Launch the FastAPI app with uvicorn
import uvicorn
uvicorn.run("my_project.api.app:app", host="0.0.0.0", port=8000, reload=True)
