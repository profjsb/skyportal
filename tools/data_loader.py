#!/usr/bin/env python

import os
import sys
import base64
import textwrap
import time
from os.path import join as pjoin

import requests
import pandas as pd
import yaml
from yaml import Loader

from baselayer.app.env import load_env, parser
from skyportal.tests import api


if __name__ == "__main__":
    parser.description = 'Load data into SkyPortal'
    parser.add_argument('data_files', type=str, nargs='+',
                        help='YAML files with data to load')
    parser.add_argument('--host',
                        help=textwrap.dedent('''Fully specified URI of the running SkyPortal instance.
                             E.g., https://myserver.com:9000.

                             Defaults to http://localhost on the port specified
                             in the SkyPortal configuration file.'''))
    parser.add_argument('--token',
                        help=textwrap.dedent('''Token required for accessing the SkyPortal API.

                             By default, SkyPortal produces a token that is
                             written to .tokens.yaml.  If no token is specified
                             here, that token will be used.'''))

    env, cfg = load_env()

    # TODO: load multiple files
    if len(env.data_files) > 1:
        raise NotImplementedError("Cannot yet handle multiple data files")

    fname = env.data_files[0]
    src = yaml.load(open(fname, "r"), Loader=Loader)
    src_path = os.path.dirname(fname)

    def get_token():
        if env.token:
            return env.token

        try:
            token = yaml.load(open('.tokens.yaml'), Loader=yaml.Loader)['INITIAL_ADMIN']
            print('Token loaded from `.tokens.yaml`')
            return token
        except (FileNotFoundError, TypeError, KeyError) as e:
            print('Error: no token specified, and no suitable token found in .tokens.yaml')
            return None

    print('Testing connection...', end='')

    RETRIES = 10
    for i in range(RETRIES):
        try:
            admin_token = get_token()

            def get(endpoint, token=admin_token):
                response_status, data = api("GET", endpoint,
                                            token=token,
                                            host=env.host)
                return response_status, data

            def post(endpoint, data, token=admin_token):
                response_status, data = api("POST", endpoint,
                                            data=data,
                                            token=token,
                                            host=env.host)
                return response_status, data

            def assert_post(endpoint, data, token=admin_token):
                response_status, data = post(endpoint, data, token)
                if not response_status == 200 and data["status"] == "success":
                    raise RuntimeError(
                        f'API call to {endpoint} failed with status {status}: {data["message"]}'
                    )
                return data

            status, data = get('sysinfo')

            if status == 200:
                break
            else:
                if i == RETRIES - 1:
                    print('FAIL')
                else:
                    time.sleep(2)
                    print('Reloading auth tokens and trying again...', end='')
                continue
        except requests.exceptions.ConnectionError:
            if i == RETRIES - 1:
                print('FAIL')
                print()
                print('Error: Could not connect to SkyPortal instance; please ensure ')
                print('       it is running at the given host/port')
                sys.exit(-1)
            else:
                time.sleep(2)
                print('Retrying connection...')

    if status not in (200, 400):
        print(f'Error: could not connect to server (HTTP status {status})')
        sys.exit(-1)

    if data['status'] != 'success':
        print('Error: Could not authenticate against SkyPortal; please specify a valid token.')
        sys.exit(-1)

    status, response = get('groups/public')
    if status != 200:
        print('Error: no public group found; aborting')
        sys.exit(-1)
    public_group_id = response['data']['id']

    error_log = []

    references = {
        'public_group_id': public_group_id
    }

    def inject_references(obj):
        if isinstance(obj, dict):
            if 'file' in obj:
                filename = pjoin(src_path, obj['file'])
                if filename.endswith('csv'):
                    df = pd.read_csv(filename)
                    obj.pop('file')
                    obj.update(df.to_dict(orient='list'))
                elif filename.endswith('.png'):
                    return base64.b64encode(open(filename, 'rb').read())
                else:
                    raise NotImplementedError(f'{filename}: Only CSV files currently supported for extending individual objects')

            for k, v in obj.items():
                obj[k] = inject_references(v)
            return obj
        elif isinstance(obj, str) and obj.startswith('='):
            try:
                return references[obj[1:]]
            except KeyError:
                print(f'\nReference {obj[1:]} not found while posting to {endpoint}; skipping')
                raise
        elif isinstance(obj, list):
            return [inject_references(item) for item in obj]
        else:
            return obj

    for endpoint, to_post in src.items():
        # Substitute references in path
        endpoint_parts = endpoint.split('/')
        try:
            for i, part in enumerate(endpoint_parts):
                if part.startswith('='):
                    endpoint_parts[i] = str(references[part[1:]])
        except KeyError:
            print(f'\nReference {part[1:]} not found while interpolating endpoint {endpoint}; skipping')
            continue

        endpoint = '/'.join(endpoint_parts)

        print(f'Posting to {endpoint}: ', end='')
        if 'file' in to_post:
            filename = pjoin(src_path, to_post['file'])
            post_objs = yaml.load(open(filename, 'r'),
                                  Loader=yaml.Loader)
        else:
            post_objs = to_post

        for obj in post_objs:
            # Fields that start with =, such as =id, get saved for using as
            # references later on
            saved_fields = {v: k[1:] for k, v in obj.items() if k.startswith('=')}

            # Remove all such fields from the object to be posted
            obj = {k: v for k, v in obj.items() if not k.startswith('=')}

            # Replace all references of the format field: =key or [=key, ..]
            # with the appropriate reference value
            try:
                inject_references(obj)
            except KeyError:
                continue

            status, response = post(endpoint, data=obj)

            print('.' if status == 200 else 'X', end='')
            if status != 200:
                error_log.append(f"/{endpoint}: {response['message']}")
                continue

            # Save all references from the response
            for target, field in saved_fields.items():
                references[target] = response['data'][field]

        print()

<<<<<<< HEAD
    if error_log:
        print("\nError log:")
        print("----------")
        print("\n".join(error_log))

        sys.exit(-1)

=======
        try:
            verify_server_availability(server_url)
            print("App running - continuing with API calls")

            if src.get("groups") is not None:
                with status("Creating groups"):
                    group_ids = []
                    group_dict = {}
                    filter_dict = {}
                    for group in src.get('groups', []):
                        data = assert_post(
                            "groups",
                            {
                                "name": group["name"],
                                "group_admins": group["group_admins"]
                            },
                            tokens[group["token"]]
                        )
                        group_ids.append(data["data"]["id"])
                        group_dict[group["name"]] = group_ids[-1]

                        for filt in group.get("filters", []):
                            data = assert_post(
                                "filters",
                                {
                                    "group_id": group_ids[-1],
                                    "query_string": filt["query_string"]
                                },
                                tokens[group["token"]]
                            )
                            filter_dict[filt["name"]] = data["data"]["id"]

                        for member in group.get("members", []):
                            data = assert_post(
                                f"groups/{group_ids[-1]}/users/{member['username']}",
                                {"admin": member.get('admin', False)},
                                tokens[group["token"]]
                            )
            if src.get("taxonomies") is not None:
                with status("Creating Taxonomies"):

                    for tax in src.get('taxonomies', []):
                        name = tax["name"]
                        provenance = tax.get("provenance")

                        if tax.get("tdtax", False):
                            hierarchy = tdtax.taxonomy
                            version = tdtax.__version__
                        else:
                            hierarchy = tax["hierarchy"]
                            tdtax.validate(hierarchy, tdtax.schema)
                            version = tax["version"]

                        group_ids = [group_dict[g] for g in tax["groups"]]
                        payload = {
                            "name": name,
                            "hierarchy": hierarchy,
                            "group_ids": group_ids,
                            "provenance": provenance,
                            "version": version,
                        }
                        data = assert_post(
                            "taxonomy",
                            payload,
                            tokens[tax["token"]]
                        )
            if src.get("telescopes") is not None:
                with status("Creating Telescopes"):
                    telescope_dict = {}
                    tel_src = src.get("telescopes")
                    if isinstance(tel_src, dict) and \
                            src["telescopes"].get("file") is not None:
                        # we're asked to load a file containing telescopes
                        fname = str(topdir / src["telescopes"]["file"])
                        tel_src = yaml.load(open(fname, "r"), Loader=Loader)
                        tel_group_ids = [group_dict[g] for g in
                                         src["telescopes"]["group_names"]]
                        tel_token = src["telescopes"]["token"]

                    for telescope in tel_src:
                        if telescope.get("group_names") is None:
                            group_ids = tel_group_ids
                        else:
                            group_ids = [group_dict[g] for g in telescope["group_names"]]
                        if telescope.get("nickname") is not None:
                            nickname = telescope["nickname"]
                        else:
                            nickname = telescope["id"]
                        if telescope.get("token") is not None:
                            token_name = telescope["token"]
                        else:
                            token_name = tel_token

                        data = assert_post(
                            "telescope",
                            data={
                                "name": telescope["name"],
                                "nickname": nickname,
                                "lat": telescope.get("lat", 0.0),
                                "lon": telescope.get("lon", 0.0),
                                "elevation": telescope.get("elevation", 0.0),
                                "diameter": telescope.get("diameter", 1.0),
                                "group_ids": group_ids
                            },
                            token=tokens[token_name]
                        )
                        telescope_dict[nickname] = data["data"]["id"]
                if src.get("instruments") is not None:
                    with status("Creating instruments"):
                        instrument_dict = {}
                        ins_src = src.get("instruments")
                        if isinstance(ins_src, dict) and \
                                src["instruments"].get("file") is not None:
                            # we're asked to load a file containing telescopes
                            fname = str(topdir / src["instruments"]["file"])
                            ins_src = yaml.load(open(fname, "r"), Loader=Loader)
                            ins_token = src["instruments"]["token"]

                        for instrument in ins_src:
                            if instrument.get("token") is not None:
                                token_name = instrument["token"]
                            else:
                                token_name = ins_token

                            if instrument.get("telescope_nickname") is not None:
                                telname = instrument["telescope_nickname"]
                            else:
                                telname = instrument.get("telescope_id")
                            telid = telescope_dict[telname]
                            # print(telid, telname, token_name, tokens[token_name], group_ids)
                            data = assert_post(
                                "instrument",
                                data={
                                    "name": instrument["name"],
                                    "type": instrument["type"],
                                    "band": instrument["band"],
                                    "telescope_id": telid,
                                    "filters": instrument.get("filters", [])
                                },
                                token=tokens[token_name]
                            )
                            instrument_dict[instrument["name"]] = data["data"]["id"]
            if src.get("sources") is not None:
                with status("Loading Source and Candidates"):
                    (basedir / "static/thumbnails").mkdir(parents=True, exist_ok=True)
                    for source in src.get("sources"):
                        sinfo = [{"id": source["id"], "ra": source["ra"],
                                  "dec": source["dec"], "save_source": True,
                                  "redshift": source.get("redshift", 0.0),
                                  "altdata": source.get("altdata", None),
                                  "cand_filts": source["candidate"]["candidate_filters"],
                                  "comments": source.get("comments", [])}]
                        if source.get("unsaved_candidate_copy") is not None:
                            sinfo.append(
                                {"id": source["unsaved_candidate_copy"]["candidate_id"],
                                 "ra": source["ra"],
                                 "dec": source["dec"], "save_source": False,
                                 "redshift": source.get("redshift", 0.0),
                                 "altdata": source.get("altdata", None),
                                 "cand_filts": source["unsaved_candidate_copy"]["candidate_filters"],
                                 "comments": source["unsaved_candidate_copy"].get("comments", [])}
                            )
                        group_ids = [group_dict[g] for g in source["group_names"]]

                        if source.get("photometry") is not None:
                            phot_file = basedir / source["photometry"]["data"]
                            phot_data = pd.read_csv(phot_file)
                            phot_instrument_name = source["photometry"]["instrument_name"]

                        if source.get("spectroscopy") is not None:
                            spec_file = basedir / source["spectroscopy"]["data"]
                            spec_data = pd.read_csv(spec_file)
                            spec_instrument_name = source["spectroscopy"]["instrument_name"]
                            observed_at = source["spectroscopy"]["observed_at"]
                        for si in sinfo:
                            if si["save_source"]:
                                data = assert_post(
                                    "sources",
                                    data={
                                        "id": si["id"],
                                        "ra": si["ra"],
                                        "dec": si["dec"],
                                        "redshift": si["redshift"],
                                        "altdata": si["altdata"],
                                        "group_ids": group_ids
                                    },
                                    token=tokens[source["token"]]
                                )
                            filter_ids = [filter_dict[f] for f in si["cand_filts"]]
                            data = assert_post(
                                "candidates",
                                data={
                                    "id": si["id"],
                                    "ra": si["ra"],
                                    "dec": si["dec"],
                                    "redshift": si["redshift"],
                                    "altdata": si["altdata"],
                                    "filter_ids": filter_ids
                                },
                                token=tokens[source["token"]]
                            )

                            for comment in si["comments"]:
                                data = assert_post(
                                    "comment",
                                    data={"obj_id": si["id"], "text": comment},
                                    token=tokens[source["token"]]
                                )

                            if source.get("photometry") is not None:
                                data = assert_post(
                                    "photometry",
                                    data={
                                        "obj_id": si['id'],
                                        "instrument_id":
                                            instrument_dict[phot_instrument_name],
                                        "mjd": phot_data.mjd.tolist(),
                                        "flux": phot_data.flux.tolist(),
                                        "fluxerr": phot_data.fluxerr.tolist(),
                                        "zp": phot_data.zp.tolist(),
                                        "magsys": phot_data.magsys.tolist(),
                                        "filter": phot_data["filter"].tolist(),
                                        "group_ids": group_ids,
                                    },
                                    token=tokens[source["token"]]
                                )

                            if source.get("spectroscopy") is not None:
                                for i, df in spec_data.groupby("instrument_id"):
                                    # TODO: spec.csv shouldn't hard code the
                                    # instrument ID. For now, use what'source
                                    # in the config for instrument
                                    data = assert_post(
                                        "spectrum",
                                        data={
                                            "obj_id": si["id"],
                                            "observed_at": observed_at,
                                            "instrument_id":
                                                instrument_dict[spec_instrument_name],
                                            "wavelengths": df.wavelength.tolist(),
                                            "fluxes": df.flux.tolist(),
                                        },
                                        token=tokens[source["token"]]
                                    )

                            if source.get("thumbnails") is not None:
                                for ttype, fname in source.get("thumbnails").items():
                                    fpath = basedir / f"{fname}"
                                    thumbnail_data = base64.b64encode(
                                        open(os.path.abspath(fpath), "rb").read()
                                    )
                                    data = assert_post(
                                        "thumbnail",
                                        data={
                                            "obj_id": si["id"],
                                            "data": thumbnail_data,
                                            "ttype": ttype,
                                        },
                                        token=tokens[source["token"]]
                                    )

                                Obj.query.get(si["id"]).add_linked_thumbnails()

        finally:
            if not app_already_running:
                print("Terminating web app")
                os.killpg(os.getpgid(web_client.pid), signal.SIGTERM)
>>>>>>> 0de51c0bba6f8ddfbe764b457693be19021570d9
