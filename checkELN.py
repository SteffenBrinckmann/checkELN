import json
import traceback
from pathlib import Path
import tempfile
from pathlib import Path
from zipfile import ZIP_DEFLATED
from zipfile import ZipFile
from rocrate.rocrate import ROCrate
from jsonschema import Draft202012Validator
from rocrate_validator import services, models
import streamlit as st

METADATA_FILE = 'ro-crate-metadata.json'

def testPypiROCrate(iFile):
    output = ''
    success = True
    try:
        with ZipFile(iFile, 'r', compression=ZIP_DEFLATED) as elnFile:
            dirpath = Path(tempfile.mkdtemp())
            elnFile.extractall(dirpath)
            tempPath= [i for i in dirpath.iterdir() if i.is_dir()][0]
            _ = ROCrate(tempPath)
    except Exception:
        output +=  traceback.format_exc()
        success = False
    return success, output


def testParamsMetadataJson(iFile):
    # global variables worth discussion
    ROCRATE_NOTE_MANDATORY = ['version','sdPublisher']
    DATASET_MANDATORY = ['name']
    DATASET_SUGGESTED = ['author','mentions',  'dateCreated', 'dateModified', 'identifier', 'text', 'keywords']
    FILE_MANDATORY = ['name']
    FILE_SUGGESTED = ['sha256', 'encodingFormat', 'contentSize', 'description']

    # runtime global variables
    OUTPUT_INFO = False
    OUTPUT_COUNTS = False
    KNOWN_KEYS = DATASET_MANDATORY+DATASET_SUGGESTED+FILE_MANDATORY+FILE_SUGGESTED+['@id', '@type']
    LABEL = 'params_metadata_json'

    def processNode(graph, nodeID):
        """
        recursive function call to process each node

        Args:
            graph: full graph
            nodeID: id of node in graph
        """
        output = ''
        globalSuccess = True
        nodes = [ i for i in graph if '@id' in i and i['@id'] == nodeID]
        if len(nodes)!=1:
            output += f'- **ERROR: all entries must only occur once in crate. check:{nodeID}\n'
            return
        node = nodes[0]
        # CHECK IF MANDATORY AND SUGGESTED KEYWORDS ARE PRESENT
        if '@type' not in node:
            output += f'- **ERROR: all nodes must have @type. check:{nodeID}\n'
            return False
        if node['@type'] == 'Dataset':
            for key in DATASET_MANDATORY:
                if key not in node:
                    output += f'- **ERROR in dataset: "{key}" not in @id={node["@id"]}\n'
                    globalSuccess = False
            for key in DATASET_SUGGESTED:
                if key not in node and OUTPUT_INFO:
                    output += f'- **INFO for dataset: "{key}" not in @id={node["@id"]}\n'
        elif node['@type'] == 'File':
            for key in FILE_MANDATORY:
                if key not in node:
                    output += f'- **ERROR in file: "{key}" not in @id={node["@id"]}\n'
                    globalSuccess = False
            for key in FILE_SUGGESTED:
                if key not in node and OUTPUT_INFO:
                    output += f'- **INFO for file: "{key}" not in @id={node["@id"]}\n'
        # CHECK PROPERTIES FOR ALL KEYS
        if any(not str(i).strip() for i in node.values()):
            output += f'- **WARNING: {nodeID} contains empty values in the key-value pairs\n'
        # SPECIFIC CHECKS ON CERTAIN KEYS
        if isinstance(node.get('keywords', ''), list):
            output += f'- **ERROR: {nodeID} contains an array of keywords. Use comma or space separated string\n'
            globalSuccess = False
        # recurse to children
        children = node.pop('hasPart') if 'hasPart' in node else []
        for child in children:
            globalSuccess = processNode(graph, child['@id']) and globalSuccess
        return globalSuccess

    # main part
    output = ''
    success = True
    with ZipFile(iFile, 'r', compression=ZIP_DEFLATED) as elnFile:
        success = True
        metadataJsonFile = [i for i in elnFile.namelist() if i.endswith(METADATA_FILE)][0]
        metadataContent = json.loads(elnFile.read(metadataJsonFile))
        graph = metadataContent["@graph"]
        # find information from master node
        ro_crate_nodes = [i for i in graph if i["@id"] == METADATA_FILE]
        if len(ro_crate_nodes) == 1:
            for key in ROCRATE_NOTE_MANDATORY:
                if key not in ro_crate_nodes[0]:
                    output += f'- **ERROR: "{key}" not in @id={METADATA_FILE}\n'
        else:
            output += f'- **ERROR: @id={METADATA_FILE} does not uniquely exist\n '
            success = False
        main_node = [i for i in graph if i["@id"] == "./"][0]

        # iteratively go through graph
        for partI in main_node['hasPart']:
            success = processNode(graph, partI['@id']) and success
        # count occurances of all keys
        counts = {}
        for node in graph:
            if node['@id'] in ['./',METADATA_FILE]:
                continue
            for key in node.keys():
                if key in counts:
                    counts[key] += 1
                else:
                    counts[key] = 1
        view = [ (v,k) for k,v in counts.items() ]
        view.sort(reverse=True)
        if OUTPUT_COUNTS:
            output += f'### Counts\n'
            for v,k in view:
                prefix = '   ' if k in KNOWN_KEYS else ' * '
                output += f'- {prefix}{k:15}: {v}\n'
    return success, output

SCHEMA = {"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"@context":{"type":"string","format":"uri"},"@graph":{"type":"array","items":{"type":"object","properties":{"@id":{"type":"string"},"@type":{"type":"string"},"about":{"type":"object","properties":{"@id":{"type":"string"}}},"conformsTo":{"type":"object","properties":{"@id":{"type":"string","format":"uri"}}},"dateCreated":{"type":"string","format":"date-time"},"sdPublisher":{"type":"object","properties":{"@id":{"type":"string"}}},"version":{"type":"string"},"author":{"type":"object","properties":{"@id":{"type":"string"}}},"dateModified":{"type":"string","format":"date-time"},"name":{"type":"string"},"encodingFormat":{"type":"string"},"url":{"type":"string","format":"uri"},"genre":{"type":"string"},"creativeWorkStatus":{"type":"string"},"identifier":{"type":"string"},"keywords":{"type":"string"},"hasPart":{"type":"array","items":{"type":"object","properties":{"@id":{"type":"string"}}}},"comment":{"type":"array","items":{"type":"object","properties":{"@id":{"type":"string"},"@type":{"type":"string"},"dateCreated":{"type":"string","format":"date-time"},"text":{"type":"string"},"author":{"type":"object","properties":{"@id":{"type":"string"}}}}}}},"required":["@id","@type"]}}},"required":["@context","@graph"]}
def testSchema(iFile):
    validator = Draft202012Validator(schema=SCHEMA)
    validator.check_schema(schema=SCHEMA)
    success = True
    output = ''
    with ZipFile(iFile, 'r', compression=ZIP_DEFLATED) as elnFile:
        metadataJsonFile = [i for i in elnFile.namelist() if i.endswith(METADATA_FILE)][0]
        metadataContent = json.loads(elnFile.read(metadataJsonFile))
        for error in sorted(validator.iter_errors(metadataContent), key=str):
            output += f'- {error.message}\n'
            success = False
    return success, output


def testValidator(iFile):
    output = ''
    success = True
    with ZipFile(iFile, 'r', compression=ZIP_DEFLATED) as elnFile:
        dirpath = Path(tempfile.mkdtemp())/"Random"
        dirpath.mkdir(parents=True, exist_ok=True)
        elnFile.extractall(dirpath)
        rocrate_dir= [i for i in dirpath.iterdir() if i.is_dir()][0]

        # start validation
        settings = services.ValidationSettings(
            rocrate_uri=rocrate_dir,
            profile_identifier='ro-crate-1.1',
            requirement_severity=models.Severity.REQUIRED,
        )
        result = services.validate(settings)
        if not result.has_issues():
            success = True
        else:
            output += '**File invalid**\n'
            for issue in result.get_issues():
                output += f"- Detected issue of severity {issue.severity.name} with check \"{issue.check.identifier}\": {issue.message}\n"
            success = False
    return success, output


if __name__ == '__main__':
    col1, col2 = st.columns([0.7, 0.3])
    col1.markdown('## Verify your .eln file')
    col1.markdown('The ELN file format is an archive format for exchange of experimental results and data. '
                  'This file format can be created and read by software such as Electronic Laboratory Notebooks. '
                  'For more information visit [link](https://github.com/TheELNConsortium/TheELNFileFormat).\n\n'
                  'Here you can easily verify the validity of each file.')
    col2.image('logo-color-fade.png')

    uploaded_file = st.file_uploader("Choose a file")
    if uploaded_file is not None:
        st.markdown("# Test summary:")

        success, output = testPypiROCrate(uploaded_file)
        with st.expander(("Success:" if success else "FAILURE:")+" Pypi RO-Crate", icon='✅' if success else '❌'):
            st.code('Success' if success else output)

        success, output = testValidator(uploaded_file)
        with st.expander(("Success:" if success else "FAILURE:")+" Validator", icon='✅' if success else '❌'):
            st.code('Success' if success else output)

        success, output = testParamsMetadataJson(uploaded_file)
        with st.expander(("Success:" if success else "FAILURE:")+" Parameters Metadata Json", icon='✅' if success else '❌'):
            st.code('Success' if success else output)

        success, output = testSchema(uploaded_file)
        with st.expander(("Success:" if success else "FAILURE:")+" Schema", icon='✅' if success else '❌'):
            st.code('Success' if success else output)
