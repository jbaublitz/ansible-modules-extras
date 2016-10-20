#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Cambridge Semantics Inc.
#
# This file is part of Ansible.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''
---
module: gcdataproc
short_description: Module for Google Cloud Dataproc clusters.
description:
  - Creates or destroys Google Cloud Dataproc clusters on Google Cloud.
version_added: "2.3"
author: "John Baublitz @jbaublitz"
requirements:
  - "python >= 2.6"
  - "google-api-python-client >= 1.5.4"
  - "oauth2client >= 3.0.0"
options:
notes:
'''

EXAMPLES = '''
# Create an A record.
- gcdns_record:
    record: 'www1.example.com'
    zone: 'example.com'
    type: A
    value: '1.2.3.4'

# Update an existing record.
- gcdns_record:
    record: 'www1.example.com'
    zone: 'example.com'
    type: A
    overwrite: true
    value: '5.6.7.8'

# Remove an A record.
- gcdns_record:
    record: 'www1.example.com'
    zone_id: 'example-com'
    state: absent
    type: A
    value: '5.6.7.8'

# Create a CNAME record.
- gcdns_record:
    record: 'www.example.com'
    zone_id: 'example-com'
    type: CNAME
    value: 'www.example.com.'    # Note the trailing dot

# Create an MX record with a custom TTL.
- gcdns_record:
    record: 'example.com'
    zone: 'example.com'
    type: MX
    ttl: 3600
    value: '10 mail.example.com.'    # Note the trailing dot

# Create multiple A records with the same name.
- gcdns_record:
    record: 'api.example.com'
    zone_id: 'example-com'
    type: A
    record_data:
      - '192.0.2.23'
      - '10.4.5.6'
      - '198.51.100.5'
      - '203.0.113.10'

# Change the value of an existing record with multiple record_data.
- gcdns_record:
    record: 'api.example.com'
    zone: 'example.com'
    type: A
    overwrite: true
    record_data:           # WARNING: All values in a record will be replaced
      - '192.0.2.23'
      - '192.0.2.42'    # The changed record
      - '198.51.100.5'
      - '203.0.113.10'

# Safely remove a multi-line record.
- gcdns_record:
    record: 'api.example.com'
    zone_id: 'example-com'
    state: absent
    type: A
    record_data:           # NOTE: All of the values must match exactly
      - '192.0.2.23'
      - '192.0.2.42'
      - '198.51.100.5'
      - '203.0.113.10'

# Unconditionally remove a record.
- gcdns_record:
    record: 'api.example.com'
    zone_id: 'example-com'
    state: absent
    overwrite: true   # overwrite is true, so no values are needed
    type: A

# Create an AAAA record
- gcdns_record:
    record: 'www1.example.com'
    zone: 'example.com'
    type: AAAA
    value: 'fd00:db8::1'

# Create a PTR record
- gcdns_record:
    record: '10.5.168.192.in-addr.arpa'
    zone: '5.168.192.in-addr.arpa'
    type: PTR
    value: 'api.example.com.'    # Note the trailing dot.

# Create an NS record
- gcdns_record:
    record: 'subdomain.example.com'
    zone: 'example.com'
    type: NS
    ttl: 21600
    record_data:
      - 'ns-cloud-d1.googledomains.com.'    # Note the trailing dots on values
      - 'ns-cloud-d2.googledomains.com.'
      - 'ns-cloud-d3.googledomains.com.'
      - 'ns-cloud-d4.googledomains.com.'

# Create a TXT record
- gcdns_record:
    record: 'example.com'
    zone_id: 'example-com'
    type: TXT
    record_data:
      - '"v=spf1 include:_spf.google.com -all"'   # A single-string TXT value
      - '"hello " "world"'    # A multi-string TXT value
'''

RETURN = '''
overwrite:
    description: Whether to the module was allowed to overwrite the record
    returned: success
    type: boolean
    sample: True
record:
    description: Fully-qualified domain name of the resource record
    returned: success
    type: string
    sample: mail.example.com.
state:
    description: Whether the record is present or absent
    returned: success
    type: string
    sample: present
ttl:
    description: The time-to-live of the resource record
    returned: success
    type: int
    sample: 300
type:
    description: The type of the resource record
    returned: success
    type: string
    sample: A
record_data:
    description: The resource record values
    returned: success
    type: list
    sample: ['5.6.7.8', '9.10.11.12']
zone:
    description: The dns name of the zone
    returned: success
    type: string
    sample: example.com.
zone_id:
    description: The Google Cloud DNS ID of the zone
    returned: success
    type: string
    sample: example-com
'''

import os
import time

try:
    from oauth2client.service_account import ServiceAccountCredentials
    from apiclient.discovery import build
    from googleapiclient.errors import HttpError
    HAS_GOOGLE_API_LIB = True
except ImportError:
    HAS_GOOGLE_API_LIB = False

def google_auth(module, cred_path):
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    credentials = None
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
                          cred_path,
                          scopes=scopes
                      )
    except Exception as e:
        module.fail_json(msg=e, changed=False)

    return credentials

def generate_resource_uri(project, *args):
    google_base_uri = \
            'https://www.googleapis.com/compute/v1/projects/%s/' % project
    return google_base_uri + '/'.join(args)

def populate_request_body(module, name, project, region, zone):
    image_version = module.params.get('image_version')
    bucket = module.params.get('bucket')
    network = module.params.get('network')
    subnetwork = module.params.get('subnetwork')
    tags = module.params.get('tags')
    service_account_scopes = module.params.get('service_account_scopes')
    metadata = module.params.get('metadata')
    init_actions = module.params.get('init_actions')
    master_config = module.params.get('master_config')
    worker_config = module.params.get('worker_config')
    second_worker_config = module.params.get('second_worker_config')

    body = {
        'clusterName': name,
        'projectId': project,
        'config': {
            'gceClusterConfig': {
                'zoneUri': generate_resource_uri(
                    project,
                    'zones',
                    zone
                )
            }
        }
    }

    if image_version:
        body['config']['softwareConfig'] = {}
        body['config']['softwareConfig']['imageVersion'] = image_version
    if bucket:
        body['config']['configBucket'] = bucket
    if tags:
        body['config']['gceClusterConfig']['tags'] = tags.split(',')
    if network:
        body['config']['gceClusterConfig']['networkUri'] = \
            generate_resource_uri(
                project,
                'regions',   
                region,
                network
            )
    if subnetwork:
        body['config']['gceClusterConfig']['subnetworkUri'] = \
            generate_resource_uri(
                project,
                'regions',
                region,
                subnetwork
            )
    if service_account_scopes:
        body['config']['gceConfigCluster']['serviceAccountScopes'] = \
            ['https://www.googleapis.com/auth/' + scope \
             for scope in service_account_scopes]
    if metadata:
        body['config']['gceConfigCluster']['metadata'] = metadata
    if init_actions:
        body['config']['initializationActions'] = init_actions
    if master_config:
        body['config']['masterConfig'] = master_config
    if worker_config:
        body['config']['workerConfig'] = worker_config
    if second_worker_config:
        body['config']['secondaryWorkerConfig'] = second_worker_config

    return body

def main():
    module = AnsibleModule(
        argument_spec = dict(
            name = dict(required=True),
            state = dict(default='present'),
            network = dict(),
            subnetwork = dict(),
            region = dict(default='us-central1'),
            zone = dict(default='us-central1-a'),
            sync = dict(type='bool', default=True),
            poll_interval = dict(type='int', default=1),
            image_version = dict(),
            service_account_scopes = dict(type='list'),
            metadata = dict(type='dict'),
            init_actions = dict(type='list'),
            master_config = dict(type='dict'),
            worker_config = dict(type='dict'),
            second_worker_config = dict(type='dict'),
            bucket = dict()
        ),
        mutually_exclusive = [
            ('network', 'subnetwork')
        ]
    )

    if not HAS_GOOGLE_API_LIB:
        module.fail_json(msg="Please install google-api-python-client library")

    name = module.params.get('name')
    state = module.params.get('state')
    region = module.params.get('region')
    zone = module.params.get('zone')
    sync = module.params.get('sync')
    poll_interval = module.params.get('poll_interval')

    if not zone.startswith(region):
        module.fail_json(msg="Region %s must contain zone %s" % (region, zone))

    gce_email = os.environ.get('GCE_EMAIL', None)
    gce_project = os.environ.get('GCE_PROJECT', None)
    gce_credentials = os.environ.get('GCE_CREDENTIALS_FILE_PATH', None)
    if not gce_email or not gce_project or not gce_credentials:
        module.fail_json(msg="Please define Google auth environment " \
                             "variables GCE_EMAIL, GCE_PROJECT and " \
                             "GCE_CREDENTIALS_FILE_PATH",
                             changed=False)

    credentials = google_auth(module, gce_credentials)
    dataproc = build('dataproc', 'v1', credentials=credentials)
    json = {}
    changed = False
    if state in ['present', 'active']:
        try:
            json = dataproc.projects().regions().clusters() \
                   .get(projectId=gce_project,
                        region='global',
                        clusterName=name).execute()
        except HttpError:
            body = populate_request_body(module, name, gce_project, region, zone)
            json = dataproc.projects().regions().clusters() \
                    .create(projectId=gce_project,
                            region='global',
                            body=body
                    ).execute()
            if sync:
                while 'status' not in json or \
                        json['status']['state'] != 'RUNNING':
                    json = dataproc.projects().regions().clusters() \
                           .get(projectId=gce_project,
                                region='global',
                                clusterName=name).execute();
                    time.sleep(poll_interval)
            changed = True
        except Exception as e:
            module.fail_json(changed=changed, msg=str(e))
    elif state in ['absent', 'deleted']:
        try:
            json = dataproc.projects().regions().clusters() \
                   .get(projectId=gce_project,
                        region='global',
                        clusterName=name).execute()
        except HttpError as e:
            pass
        except Exception as e:
            module.fail_json(changed=changed, msg=str(e))
        else:
            json = dataproc.projects().regions().clusters() \
                   .delete(projectId=gce_project,
                           region='global',
                           clusterName=name
                   ).execute()
            changed = True

    module.exit_json(changed=changed, **json)

# import module snippets
from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()
