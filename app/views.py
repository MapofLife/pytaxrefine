try:
    import simplejson as json
except ImportError:
    import json

import requests

from app import app
from flask import request, jsonify

metadata = {
    "name": "GBIF Reconciliation Service",
    "identifierSpace": "http://www.gbif.org/species/",
    "schemaSpace": "http://rs.tdwg.org/dwc/terms/",
    "view": {
        "url": "http://www.gbif.org/species/{{id}}#overview"
    },
    "preview": {
        "url": "http://www.gbif.org/species/{{id}}#overview",
        "width": 700,
        "height": 350
    },
    "defaultTypes": []
}


def search(query):
    results = get_gbif_match_all(query)

    if len(results) == 0:
        print("  > No matches found, carrying out full-text search instead.")
        results = get_gbif_full_text_matches_for_name(query)

    print("Retrieved %d matches for '%s'" % (len(results), query))

    # SKIPPED: Filter out on the basis of kingdom.

    summarized = summarize_name_usages(results)

    # Sort summarized results based on the score
    sorted_results = sorted(summarized, key=lambda x: x['score'], reverse=True)

    return {'result': sorted_results}


def get_gbif_match_all(name):
    results = gbif_match_search(name, 0, 200)

    try:
        results = results['results']
        gbif_results = []
        for result in results:
            gbif_results.append(result)

        return gbif_results
    except KeyError:
        return None


def gbif_match_search(name, offset, limit):
    params = {
        "name": name,
        "offset": offset,
        "limit": limit
    }
    r = requests.get('http://api.gbif.org/v1/species', params=params, timeout=60)
    if r.status_code == 200:
        return r.json()

    return None


def gbif_ft_search(name, offset, limit):
    params = {
        "q": name,
        "offset": offset,
        "limit": limit
    }
    r = requests.get('http://api.gbif.org/v1/species/search', params=params, timeout=60)
    if r.status_code == 200:
        return r.json()

    return None


def get_gbif_full_text_matches_for_name(name):
    matches = []

    # Limit total to 200 records.
    response = gbif_ft_search(name, 0, 200)
    total = response['count']

    for result in response['results']:
        if ('scientificName' in result and result['scientificName'] == name) or (
                'canonicalName' in result and result['canonicalName'] == name):
            matches.append(result)

    for match in matches:
        # Rename 'key' to 'usageKey' for consistency with /lookup/name_usage
        match['usageKey'] = match['key']

        # We need a 'relatedToUsageKey' to search on. Unfortunately,
        # actually figuring out the best GBIF Nub match would take
        # a fairly long time.
        match['relatedToUsageKey'] = match['key']

    return matches


def summarize_name_usages(results):
    unique_matches = {}
    tl = {}
    summarized = []

    for match in results:

        name = match.get('canonicalName', match.get('scientificName', ''))
        accepted_name = match.get('acceptedNameUsage', match.get('accepted', ''))
        authority = match.get('authorship', 'unknown')
        kingdom = match.get('kingdom', 'Life')

        # Check if we already have a key
        ukey = '%s___%s___%s___%s' % (name, accepted_name, authority, kingdom)
        if ukey not in unique_matches:
            unique_matches[ukey] = []
        unique_matches[ukey].append(match)

        tk = (name, accepted_name, authority, kingdom)
        if tk not in tl:
            tl[tk] = []
        tl[tk].append(match)

    unique_matches = tl
    skeys = sorted(tl)

    for key in skeys:
        matches = unique_matches.get(key)  # str(key)
        gbif_keys = []
        summary = {}
        for match in matches:
            gbif_keys.append(match['key'])

            for mk in match:
                mv = match[mk]  # str(mk)
                kk = '%s___%s' % (mk, mv)
                if kk not in summary:
                    summary[kk] = 1
                else:
                    summary[kk] += 1

        # Sort ascending numerically, so the smallest
        # (i.e. oldest) GBIF ID is more likely to be used.
        gbif_keys = sorted(gbif_keys)

        if len(gbif_keys) == 0:
            print("No gbif_key provided!")
            return None

        gbif_key = gbif_keys[0]

        # Further simplify fields common for ALL checklists.
        match_count = len(matches)
        summary_tmp = {}
        for sf in summary:
            count = summary[sf]
            sff = sf.split('___')

            if count == match_count:
                summary_tmp[sff[0]] = sff[1]
            else:
                if sff[0] not in summary_tmp:
                    summary_tmp[sff[0]] = []
                summary_tmp[sff[0]].append(sff[1])
        summary = summary_tmp

        result = {}
        # akeys = key.split('___')
        result['id'] = gbif_key
        result['name'] = key[0]
        if len(key[2]) > 0:
            result['name'] += ' %s' % key[2]
        if len(key[1]) > 0:
            result['name'] += ' [=> %s]' % key[1]
        result['name'] += ' (%s)' % key[3]
        result['type'] = ['http://www.gbif.org/species/']
        result['score'] = len(matches)
        result['match'] = False
        result['summary'] = summary

        summarized.append(result)

    return summarized


def jsonpify(obj):
    """
    Wrap the response in a JSONP callback
    """
    try:
        callback = request.args['callback']
        response = app.make_response("%s(%s)" % (callback, json.dumps(obj)))
        response.mimetype = "text/javascript"
        return response
    except KeyError:
        return jsonify(obj)


@app.route('/reconcile', methods=['GET', 'POST'])
def reconcile():
    query = request.form.get('query')
    if query:
        print('Got query')
        print(query)
        if query.startswith("{"):
            query = json.loads(query)['query']
        results = search(query)
        return jsonpify(results)  # jsonpify({"result": results})

    queries = request.form.get('queries')
    if queries:
        print('Got queries')
        print(queries)
        queries = json.loads(queries)
        results = {}
        for (key, query) in queries.items():
            # results[key] = {"result": search(query['query'])}
            results[key] = search(query['query'])

        return jsonpify(results)

    return jsonpify(metadata)


@app.route('/')
@app.route('/index')
def index():
    return 'Please head to <a href="/reconcile">reconciliation service</a>'
