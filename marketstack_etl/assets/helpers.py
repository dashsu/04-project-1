from sqlalchemy import MetaData

def get_schema_metadata(engine):
    # Get the metadata of a DB
    metadata = MetaData(bind=engine)
    metadata.reflect() 
    return metadata