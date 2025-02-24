from dotenv import load_dotenv
import configparser
import os
import tableau_migration
from tableau_migration import (
    MigrationManifestSerializer,
    MigrationManifest,
    ContentFilterBase,
    ContentMigrationItem,
    IProject,
    IWorkbook,
    IDataSource
)

load_dotenv()

class ProjectWorkbookFilter(ContentFilterBase[IWorkbook]):
    def __init__(self, project_name: str):
        self.project_name = project_name
    
    def should_migrate(self, item: ContentMigrationItem[IWorkbook]) -> bool:
        return item.source_item.project_name == self.project_name

class ProjectDataSourceFilter(ContentFilterBase[IDataSource]):
    def __init__(self, project_name: str):
        self.project_name = project_name
    
    def should_migrate(self, item: ContentMigrationItem[IDataSource]) -> bool:
        return item.source_item.project_name == self.project_name

def migrate_project_content(project_name: str):
    """
    Performs a migration of workbooks and datasources from a specific project.
    
    Args:
        project_name: The name of the project to migrate from
    """
    current_file_path = os.path.abspath(__file__)
    manifest_path = os.path.join(os.path.dirname(current_file_path), 'manifest.json')
    
    # Initialize the migration builder
    plan_builder = tableau_migration.MigrationPlanBuilder()
    migration = tableau_migration.Migrator()
    
    # Read configuration
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # Create content filters
    workbook_filter = ProjectWorkbookFilter(project_name)
    datasource_filter = ProjectDataSourceFilter(project_name)
    
    # Build the migration plan
    plan_builder = (plan_builder
        .from_source_tableau_server(
            server_url=config['SOURCE']['URL'],
            site_content_url=config['SOURCE']['SITE_CONTENT_URL'],
            access_token_name=config['SOURCE']['ACCESS_TOKEN_NAME'],
            access_token=os.environ.get('TABLEAU_MIGRATION_SOURCE_TOKEN', 
                                      config['SOURCE']['ACCESS_TOKEN']))
        .to_destination_tableau_cloud(
            pod_url=config['DESTINATION']['URL'],
            site_content_url=config['DESTINATION']['SITE_CONTENT_URL'],
            access_token_name=config['DESTINATION']['ACCESS_TOKEN_NAME'],
            access_token=os.environ.get('TABLEAU_MIGRATION_DESTINATION_TOKEN',
                                      config['DESTINATION']['ACCESS_TOKEN']))
        .for_server_to_cloud()
        .with_tableau_id_authentication_type()
        .with_tableau_cloud_usernames(config['USERS']['EMAIL_DOMAIN']))
    
    # Add the filters using the correct API method
    plan_builder.add_content_filter(workbook_filter)
    plan_builder.add_content_filter(datasource_filter)
    
    # Validate the plan
    validation_result = plan_builder.validate()
    if not validation_result.is_valid:
        print("Migration plan validation failed:")
        for error in validation_result.errors:
            print(f"- {error}")
        return
    
    # Build and execute the plan
    plan = plan_builder.build()
    results = migration.execute(plan)
    
    # Print results
    print(f"\nMigration Results for project: {project_name}")
    print("----------------------------------------")
    
    for content_type in ['Workbook', 'DataSource']:
        entries = results.manifest.entries.for_content_type(content_type)
        success_count = sum(1 for e in entries if e.status.value == "Migrated")
        total_count = len(entries)
        
        print(f"\n{content_type}s:")
        print(f"Successfully migrated: {success_count}/{total_count}")
        
        # Print any errors
        errors = [e for e in entries if e.status.value == "Error"]
        if errors:
            print("\nErrors:")
            for error in errors:
                print(f"- {error.name}: {error.error_message}")

if __name__ == '__main__':
    project_name = input("Enter the name of the project to migrate: ")
    migrate_project_content(project_name)