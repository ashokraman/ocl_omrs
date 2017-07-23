"""
Command to using concept dictionary JSON files created from OpenMRS v1.11 concept dictionary into Bahmni.

Example usage:

    manage.py sync_bahmni_db --org_id=CIEL --source_id=CIEL --concept_file=CONCEPT_FILENAME --mapping_file=MAPPING_FILENAME

Set verbosity to 0 (e.g. '-v0') to suppress the results summary output. Set verbosity to 2
to see all debug output.

NOTES:
- Does not handle the OpenMRS drug table -- it is ignored for now

BUGS:

"""

from optparse import make_option
import json, uuid
from django.core.management import BaseCommand, CommandError
from omrs.models import Concept, ConceptName, ConceptClass, ConceptAnswer, ConceptSet,  ConceptReferenceSource, ConceptDescription, ConceptNumeric, ConceptReferenceTerm, ConceptReferenceMap, ConceptMapType, ConceptDatatype
from omrs.management.commands import OclOpenmrsHelper, ConceptHelper, UnrecognizedSourceException
import requests, datetime


class Command(BaseCommand):
    """
    Synchronize Bahmni/OpenMRS DB with concepts and mappping using OCL formatted json files
    """

    # Command attributes
    help = 'Synchronize Bahmni/OpenMRS DB with concepts and mappping'
    option_list = BaseCommand.option_list + (
        make_option('--concept_file',
                    action='store',
                    dest='concept_filename',
                    default=None,
                    help='OCL concept filename'),
        make_option('--mapping_file',
                    action='store',
                    dest='mapping_filename',
                    default=None,
                    help='OCL mapping filename'),
        make_option('--concept_id',
                    action='store',
                    dest='concept_id',
                    default=None,
                    help='ID for concept to sync, if specified only sync this one. e.g. 5839'),
        make_option('--retired',
                    action='store_true',
                    dest='retire_sw',
                    default=False,
                    help='If specify, output a list of retired concepts.'),
        make_option('--org_id',
                    action='store',
                    dest='org_id',
                    default=None,
                    help='org_id that owns the dictionary being imported (e.g. WHO)'),
        make_option('--source_id',
                    action='store',
                    dest='source_id',
                    default=None,
                    help='source_id of dictionary being imported (e.g. ICD-10-WHO)'),
        make_option('--check_sources',
                    action='store_true',
                    dest='check_sources',
                    default=False,
                    help='Validates that all reference sources in OpenMRS have been defined in OCL.'),
        make_option('--env',
                    action='store',
                    dest='ocl_api_env',
                    default='production',
                    help='Set the target for reference source validation to "dev", "staging", or "production"'),
        make_option('--token',
                    action='store',
                    dest='token',
                    default=None,
                    help='OCL API token to validate OpenMRS reference sources'),
    )

    OCL_API_URL = {
        'dev': 'http://api.dev.openconceptlab.com/',
        'staging': 'http://api.staging.openconceptlab.com/',
        'production': 'http://api.openconceptlab.com/',
    }



    ## EXTRACT_DB COMMAND LINE HANDLER AND VALIDATION

    def handle(self, *args, **options):
        """
        This method is called first directly from the command line, handles options, and calls
        either sync_db() or ??() depending on options set.
        """

        # Handle command line arguments
        self.org_id = options['org_id']
        self.source_id = options['source_id']
        self.concept_id = options['concept_id']
        self.concept_filename = options['concept_filename']
        self.mapping_filename = options['mapping_filename']

        self.do_retire = options['retire_sw']

        self.verbosity = int(options['verbosity'])
        self.ocl_api_token = options['token']
        if options['ocl_api_env']:
            self.ocl_api_env = options['ocl_api_env'].lower()

        # Option debug output
        if self.verbosity >= 2:
            print 'COMMAND LINE OPTIONS:', options

        # Validate the options
        self.validate_options()

        # Load the concepts and mapping file into memory
        # NOTE: This will only work if it can fit into memory -- explore streaming partial loads

        concepts = []
        mappings = []
        for line in open(self.concept_filename, 'r'):
            concepts.append(json.loads(line))
        for line in open(self.mapping_filename, 'r'):
            mappings.append(json.loads(line))

        # Initialize counters
        self.cnt_total_concepts_processed = 0
        self.cnt_concepts_created = 0
        self.cnt_internal_mappings_created = 0
        self.cnt_external_mappings_created = 0
        self.cnt_ignored_self_mappings = 0
        self.cnt_questions_created = 0
        self.cnt_answers_created = 0
        self.cnt_retired_concepts_created = 0
        self.cnt_set_members_created = 0
        self.cnt_retired_concepts_created = 0

        # Process concepts, mappings, or retirement script
        self.sync_db(concepts, mappings)

        # Display final counts
        if self.verbosity:
            self.print_debug_summary()

    def validate_options(self):
        """
        Returns true if command line options are valid, false otherwise.
        Prints error message if invalid.
        """
        # If concept/mapping export enabled, org/source IDs are required & must be valid mnemonics
        # TODO: Check that org and source IDs are valid mnemonics
        # TODO: Check that specified org and source IDs exist in OCL
        if (not self.concept_filename or not self.mapping_filename):
            raise CommandError(
                ("ERROR: concept and mapping json file names are required options "))
        if self.ocl_api_env not in self.OCL_API_URL:
            raise CommandError('Invalid "env" option provided: %s' % self.ocl_api_env)
        return True

    def print_debug_summary(self):
        """ Outputs a summary of the results """
        print '------------------------------------------------------'
        print 'SUMMARY'
        print '------------------------------------------------------'
        print 'Total concepts processed: %d' % self.cnt_total_concepts_processed
        print '------------------------------------------------------'



    ## REFERENCE SOURCE VALIDATOR

    def check_sources(self):
        """ Validates that all reference sources in OpenMRS have been defined in OCL. """
        url_base = self.OCL_API_URL[self.ocl_api_env]
        headers = {'Authorization': 'Token %s' % self.ocl_api_token}
        reference_sources = ConceptReferenceSource.objects.all()
        reference_sources = reference_sources.filter(retired=0)
        enum_reference_sources = enumerate(reference_sources)
        for num, source in enum_reference_sources:
            source_id = OclOpenmrsHelper.get_ocl_source_id_from_omrs_id(source.name)
            if self.verbosity >= 1:
                print 'Checking source "%s"' % source_id

            # Check that source exists in the source directory (which maps sources to orgs)
            org_id = OclOpenmrsHelper.get_source_owner_id(ocl_source_id=source_id)
            if self.verbosity >= 1:
                print '...found owner "%s" in source directory' % org_id

            # Check that org:source exists in OCL
            if self.ocl_api_token:
                url = url_base + 'orgs/%s/sources/%s/' % (org_id, source_id)
                r = requests.head(url, headers=headers)
                if r.status_code != requests.codes.OK:
                    raise UnrecognizedSourceException('%s not found in OCL.' % url)
                if self.verbosity >= 1:
                    print '...found %s in OCL' % url
            elif self.verbosity >= 1:
                print '...no api token provided, skipping check on OCL.'

        return True



    ## MAIN EXPORT LOOP

    def sync_db(self, concepts, mappings):
        """
        Main loop to sync all concepts and/or their mappings.

        Loop thru all concepts and mappings and generates needed entries.
        Note that the retired status of concepts is not handled here.
        """

        # Create the concept enumerator, applying 'concept_id'
        if self.concept_id is not None:
            # If 'concept_id' option set, fetch a single concept and convert to enumerator
            concept_enumerator = enumerate([concepts])
        else:
            # Fetch all concepts
            concept_enumerator = enumerate(concepts)

        # Iterate concept enumerator and process them

        for num, concept in concept_enumerator:
            self.cnt_total_concepts_processed += 1
            export_data = ''
            self.sync_concept(concept)

        self.sync_mapping(concepts, mappings)

    ## CONCEPT and MAPPINGS sync to DB

    def sync_concept(self, concept):
        """
        Create one concept and its mappings.

        :param concept: Concept to write to OpenMRS database and list of mappings.
        :returns: None.

        Note:
        - OMRS does not have locale_preferred or description_type metadata, so these are omitted
        """

        # Iterate the concept export counter
        self.cnt_concepts_created += 1

        # Concept class, check if it is already created
        concept_classes = ConceptClass.objects.filter(name=concept['concept_class'])
        concept_class = None
        if len(concept_classes) !=0:
            concept_class = concept_classes[0]
        else:
            uuidcc = uuid.uuid1()
            concept_class = ConceptClass(name=concept['concept_class'], retired=concept['retired'], creator=1, date_created=datetime.datetime.now(), uuid=uuidcc)
            concept_class.save()

        datatypes = ConceptDatatype.objects.filter(name=concept['datatype'])
        datatype = None
        if len(datatypes) !=0:
            datatype = datatypes[0]
        else:
            datatype = ConceptDatatype(name=concept['datatype'], creator=1, date_created=datetime.datetime.now())
            datatype.save()

        # Concept Name, check if it is already there
        cnames = concept['names']
        cconcept = None
        concept['is_set'] = 0
        if 'is_set' in concept['extras']:
            concept['is_set'] = concept['extras']['is_set']
        cconcept = None
        for cname in cnames:
            concept_names = ConceptName.objects.filter(name=cname['name'], locale=cname['locale'], locale_preferred=cname['locale_preferred'])
            if len(concept_names) != 0:
                cconceptname = concept_names[0]
                cconcept = Concept.objects.get(concept_id=cconceptname.concept_id)
                if cconcept is None:
                    print "Should not be here!!"
#                   cconcept = Concept(concept_id=cconceptname.concept_id, concept_class=concept_class,datatype=datatype,is_set=concept['is_set'],uuid=concept['external_id'],retired=concept['retired'], class_id=concept_class['concept_class_id'], creator=1, date_created=datetime.datetime.now())
#                   cconcept.save()
            else:
                if cconcept is None:
                    desc = None
                    if 'description' in concept:
                        desc = concept['description']
                    cconcept = Concept(concept_class=concept_class,datatype=datatype,is_set=concept['is_set'],uuid=concept['external_id'],retired=concept['retired'],creator=1,date_created=datetime.datetime.now(), description=desc)
                    cconcept.save()
                cconceptname = ConceptName(concept=cconcept, name=cname['name'], uuid=cname['external_id'], concept_name_type=cname['name_type'], locale=cname['locale'], locale_preferred=cname['locale_preferred'], creator=1, voided=0, date_created=datetime.datetime.now())
                cconceptname.save()
                # save the new id
        concept['new_id'] = cconcept.concept_id

        # Concept Descriptions
        
        for cdescription in concept['descriptions']:
            concept_description = ConceptDescription.objects.filter(concept_id=cconcept.concept_id, description=cdescription['description'])
            if len(concept_description) == 0:
                concept_description = ConceptDescription(concept_id=cconcept.concept_id, description=cdescription['description'], uuid=cdescription['external_id'], locale=cdescription['locale'], creator=1, date_created=datetime.datetime.now())
                concept_description.save()

        extra = None
        if concept['datatype'] == "Numeric":
            extra = concept['extras']
        # If the concept is of numeric type, map concept's numeric type data as extras
        if extra is not None:
            numeric = ConceptNumeric(concept_id=cconcept.concept_id)
            if numeric is None:
                numeric = ConceptNumeric(concept_id=cconcept['concept_id'], hi_absolute = extra['hi_absolute'], hi_critical=extra['hi_critical'], hi_normal=extra['hi_normal'], low_absolute=extra['low_absolute'], low_normal=extra['low_normal'], units =extra['units'],precise=extra['precise'],display_precision=extra['display_precision'], creator=1, date_created=datetime.datetime.now())
                numeric.save()

                
        # for the Mappings
    def sync_mapping(self, concepts, mappings):
        
        for ref_map in mappings:
            if 'to_concept_url' in ref_map:
                self.create_internal_mapping(concepts, map_type=ref_map['map_type'],
                    from_concept_url=ref_map['from_concept_url'],
                    to_concept_url=ref_map['to_concept_url'],
                    external_id=ref_map['external_id'])
            if 'to_source_url' in ref_map:
                self.create_external_mapping(concepts, map_type=ref_map['map_type'],from_concept_url=ref_map['from_concept_url'],to_source_url=ref_map['to_source_url'],
                    to_concept_code=ref_map['to_concept_code'],
                    external_id=ref_map['external_id'])

            
        return

    ## MAPPING EXPORT

    def export_all_mappings_for_concept(self, concept, export_qanda=True, export_set_members=True):
        """
        Export mappings for the specified concept, including its set members and linked answers.

        OCL stores all concept relationships as mappings, so OMRS mappings, Q-AND-A and
        CONCEPT-SETS are all handled here and exported as mapping JSON.
        :param concept: Concept with the mappings to export from OpenMRS database.
        :returns: List of OCL-formatted mapping dictionaries for the concept.
        """
        maps = []

        # Import OpenMRS mappings
        new_maps = self.export_concept_mappings(concept)
        if new_maps:
            maps += new_maps

        # Import OpenMRS Q&A
        if export_qanda:
            new_maps = self.export_concept_qanda(concept)
            if new_maps:
                maps += new_maps

        # Import OpenMRS Concept Set Members
        if export_set_members:
            new_maps = self.export_concept_set_members(concept)
            if new_maps:
                maps += new_maps

        return maps

    def export_concept_mappings(self, concept):
        """
        Generate OCL-formatted mappings for the concept, excluding set members and Q/A.

        Creates both internal and external mappings, based on the mapping definition.
        :param concept: Concept with the mappings to export from OpenMRS database.
        :returns: List of OCL-formatted mapping dictionaries for the concept.
        """
        export_data = []
        for ref_map in concept.conceptreferencemap_set.all():
            map_dict = None

            # Internal Mapping
            if ref_map.concept_reference_term.concept_source.name == self.org_id:
                if str(concept.concept_id) == ref_map.concept_reference_term.code:
                    # mapping to self, so ignore
                    self.cnt_ignored_self_mappings += 1
                    continue
                map_dict = self.generate_internal_mapping(
                    map_type=ref_map.map_type.name,
                    from_concept=concept,
                    to_concept_code=ref_map.concept_reference_term.code,
                    external_id=ref_map.concept_reference_term.uuid)
                self.cnt_internal_mappings_created += 1

            # External Mapping
            else:
                # Prepare to_source_id
                omrs_to_source_id = ref_map.concept_reference_term.concept_source.name
                to_source_id = OclOpenmrsHelper.get_ocl_source_id_from_omrs_id(omrs_to_source_id)
                to_org_id = OclOpenmrsHelper.get_source_owner_id(ocl_source_id=to_source_id)

                # Generate the external mapping dictionary
                map_dict = self.generate_external_mapping(
                    map_type=ref_map.map_type.name,
                    from_concept=concept,
                    to_org_id=to_org_id,
                    to_source_id=to_source_id,
                    to_concept_code=ref_map.concept_reference_term.code,
                    to_concept_name=ref_map.concept_reference_term.name,
                    external_id=ref_map.uuid)

                self.cnt_external_mappings_created += 1

            if map_dict:
                export_data.append(map_dict)

        return export_data

    def export_concept_qanda(self, concept):
        """
        Generate OCL-formatted mappings for the linked answers in this concept.
        In OpenMRS, linked answers are always internal mappings.
        :param concept: Concept with the linked answers to export from OpenMRS database.
        :returns: List of OCL-formatted mapping dictionaries representing the linked answers.
        """
        if not concept.question_answer.count():
            return []

        # Increment number of concept questions prepared for export
        self.cnt_questions_created += 1

        # Export each of this concept's linked answers as an internal mapping
        maps = []
        for answer in concept.question_answer.all():
            map_dict = self.generate_internal_mapping(
                map_type=OclOpenmrsHelper.MAP_TYPE_Q_AND_A,
                from_concept=concept,
                to_concept_code=answer.answer_concept.concept_id,
                external_id=answer.uuid)
            maps.append(map_dict)
            self.cnt_answers_created += 1

        return maps

    def export_concept_set_members(self, concept):
        """
        Generate OCL-formatted mappings for the set members in this concept.
        In OpenMRS, set members are always internal mappings.
        :param concept: Concept with the set members to export from OpenMRS database.
        :returns: List of OCL-formatted mapping dictionaries representing the set members.
        """
        if not concept.conceptset_set.count():
            return []

        # Iterate number of concept sets prepared for export
        self.cnt_retired_concepts_created += 1

        # Export each of this concept's set members as an internal mapping
        maps = []
        for set_member in concept.conceptset_set.all():
            map_dict = self.generate_internal_mapping(
                map_type=OclOpenmrsHelper.MAP_TYPE_CONCEPT_SET,
                from_concept=concept,
                to_concept_code=set_member.concept.concept_id,
                external_id=set_member.uuid)
            maps.append(map_dict)
            self.cnt_set_members_created += 1

        return maps

    def create_internal_mapping(self, concepts, map_type, from_concept_url,
                                  to_concept_url, external_id,
                                  retired=False):
        """ Generate OCL-formatted dictionary for an internal mapping based on passed params. """
        to_source=None
        from_source=None
        ciel_id = None
        iad_id = None
        if map_type == OclOpenmrsHelper.MAP_TYPE_Q_AND_A:
            s1 = from_concept_url.split("/")
            concept_id=s1[6]
            from_source=s1[4]
            new_concept_id = ConceptHelper.get_new_id(concepts, int(concept_id))
            s2 = to_concept_url.split("/")
            answer_concept=s2[6]
            to_source=s2[4]
            new_answer_concept = ConceptHelper.get_new_id(concepts, int(answer_concept))
            canswers = ConceptAnswer.objects.filter(question_concept_id=new_concept_id, answer_concept_id=new_answer_concept, uuid=external_id)
            ciel_id = concept_id
            iad_id = new_answer_concept
            if len(canswers) != 0:
                canswer = canswers[0]
            else:
                canswer = ConceptAnswer(question_concept_id=new_concept_id, answer_concept_id=new_answer_concept, uuid=external_id, creator=1, date_created=datetime.datetime.now())
                canswer.save()
        elif map_type == OclOpenmrsHelper.MAP_TYPE_CONCEPT_SET:
            s1 = from_concept_url.split("/")
            concept_set_id=s1[6]
            from_source=s1[4]
            new_concept_set_id = ConceptHelper.get_new_id(concepts, int(concept_set_id))
            s2 = to_concept_url.split("/")
            concept_id=s2[6]
            to_source=s2[4]
            new_concept_id = ConceptHelper.get_new_id(concepts, int(concept_id))

            csets = ConceptSet.objects.filter(concept_id=new_concept_id,  concept_set_owner_id=new_concept_set_id, uuid=external_id)
            if len(csets) != 0:
                cset = csets[0]
            else:
                cset = ConceptSet(concept_id=new_concept_id,  concept_set_owner_id=new_concept_set_id, uuid=external_id, creator=1, date_created=datetime.datetime.now())
                cset.save()
            ciel_id = concept_set_id
            iad_id = new_concept_set_id
        else:
            cnt_ocl_mapref += 1

        self.create_ciel_mapping(to_source, ciel_id, "SAME-AS", iad_id, external_id)
  
        return

    def create_external_mapping(self, concepts, map_type, from_concept_url,
                                  to_source_url,
                                  to_concept_code,
                                  external_id, retired=False):
        """ Generate OCL-formatted dictionary for an external mapping based on passed params. """
        ss = to_source_url.split("/")
        source_name = ss[4]
        source_id = OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(source_name)
        cc = from_concept_url.split("/")
        concept_id = cc[6]
        new_concept_id = ConceptHelper.get_new_id(concepts, int(concept_id))
        if self.verbosity >= 1:
            print 'Checking source "%s" at uuid "%s"' % (source_id, external_id)
        creference_sources = ConceptReferenceSource.objects.filter(name=source_id)
        if len(creference_sources) != 0:
            creference_source = creference_sources[0]
        else:
            creference_source = ConceptReferenceSource(name=source_id, hl7_code=None, creator=1, retired=False,uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_source.save()

        creference_terms = ConceptReferenceTerm.objects.filter(code=to_concept_code, concept_source=creference_source)
        if len(creference_terms) != 0:
            creference_term = creference_terms[0]
        else:
            creference_term = ConceptReferenceTerm(code=to_concept_code, concept_source=creference_source, creator=1, retired=False, uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_term.save()
        
        creference_map_types = ConceptMapType.objects.filter(name=map_type)
        if len(creference_map_types) != 0:
            creference_map_type = creference_map_types[0]
        else:
            creference_map_type = ConceptMapType(name=map_type, creator=1, uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_map_type.save()

        creference_maps = ConceptReferenceMap.objects.filter(concept_id=new_concept_id, concept_reference_term=creference_term, map_type_id=creference_map_type.concept_map_type_id)
        if len(creference_maps) != 0:
            creference_map = creference_maps[0]
        else:
            creference_map = ConceptReferenceMap(concept_reference_term_id=creference_term.concept_reference_term_id, concept_id=new_concept_id, uuid=external_id, map_type_id=creference_map_type.concept_map_type_id, creator=1, date_created=datetime.datetime.now())
            creference_map.save()

        self.create_ciel_mapping(cc[4], int(concept_id), map_type, new_concept_id, uuid.uuid1())

        return

    ### create CIEL mapping

    def create_ciel_mapping(self, to_source, ciel_id, map_type, iad_id, external_id):

        source_id = OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(to_source)
        creference_sources = ConceptReferenceSource.objects.filter(name=source_id)
        if len(creference_sources) != 0:
            creference_source = creference_sources[0]
        else:
            creference_source = ConceptReferenceSource(name=source_id, hl7_code=None, creator=1, retired=False,uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_source.save()

        creference_terms = ConceptReferenceTerm.objects.filter(code=ciel_id, concept_source=creference_source)
        if len(creference_terms) != 0:
            creference_term = creference_terms[0]
        else:
            creference_term = ConceptReferenceTerm(code=ciel_id, concept_source=creference_source, creator=1, retired=False, uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_term.save()
        
        creference_map_types = ConceptMapType.objects.filter(name=map_type)
        if len(creference_map_types) != 0:
            creference_map_type = creference_map_types[0]
        else:
            creference_map_type = ConceptMapType(name=map_type, creator=1, uuid=uuid.uuid1(), date_created=datetime.datetime.now())
            creference_map_type.save()

        creference_maps = ConceptReferenceMap.objects.filter(concept_id=iad_id, concept_reference_term=creference_term, map_type=creference_map_type)
        if len(creference_maps) != 0:
            creference_map = creference_maps[0]
        else:
            creference_map = ConceptReferenceMap(concept_reference_term=creference_term, concept_id=iad_id, uuid=external_id, map_type_id=creference_map_type.concept_map_type_id, creator=1, date_created=datetime.datetime.now())
            creference_map.save()
        return
        
    ### RETIRED CONCEPT EXPORT

    def export_concept_id_if_retired(self, concept):
        """ Returns the concept's ID if it is retired, None otherwise. """
        if concept.retired:
            self.cnt_retired_concepts_created += 1
            return concept.concept_id
        return None



## HELPER METHOD

def add_f(dictionary, key, value):
    """Utility function: Adds new field to the dictionary if value is not None"""
    if value is not None:
        dictionary[key] = value
