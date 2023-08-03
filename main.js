const Apify = require('apify');
const _ = require('lodash');

function createKey(result, idAttr) {
    return result ? (
        Array.isArray(idAttr) ? 
        idAttr.map(ida => result[ida]).join('_') : 
        result[idAttr]
    ) : null;
}

async function loadResults(datasetId, offset) {  
    const limit = 10000;
    if (!offset) { offset = 0; }
    const newItems = await Apify.client.datasets.getItems({
        datasetId, 
        offset,
        limit
    });

    return newItems;
}

async function createCompareMap(execId, idAttr) {
    const data = {};
    let offset = 0;
    let processed = 0;

    while (true) {
        const fullResults = await loadResults(execId, offset);
        const results = _.chain(fullResults.items).flatten().value();

        if (results.length === 0) {
            break;
        }

        _.forEach(results, (result, index) => {
            const key = createKey(result, idAttr);
            if (key) {
                data[key] = result;
            }
        });

        processed += results.length;
        offset += results.length;
    }

    return data;
}

async function compareDatasets(oldDatasetId, newDatasetId, idAttr, settings) {
    const compareMap = await createCompareMap(oldDatasetId, idAttr);
    let offset = 0;
    let processed = 0;
    let newData = [];

    while (true) {
        const fullResults = await loadResults(newDatasetId, offset);
        const results = _.chain(fullResults.items).flatten().value();

        if (results.length === 0) {
            break;
        }

        for (const result of results) {
            const id = createKey(result, idAttr);
            const oldResult = id ? compareMap[id] : null;

            if (!oldResult) {
                if (settings.addStatus) { result[settings.statusAttr] = 'NEW'; }
                if (settings.returnNew) { newData.push(result); }
            } else if (!_.isEqual(result, oldResult)) {
                const changes = getChangeAttributes(oldResult, result);
                const intersection = _.intersection(settings.updatedIf, changes);

                if (!intersection.length) {
                    if (settings.addStatus) { result[settings.statusAttr] = 'UNCHANGED'; }
                    if (settings.returnUnc) { newData.push(result); }
                } else {
                    if (settings.addStatus) { result[settings.statusAttr] = 'UPDATED'; }
                    if (settings.returnUpd) {
                        if (settings.addChanges) {
                            result[settings.changesAttr] = settings.stringifyChanges ? intersection.join(', ') : intersection;
                        }
                        newData.push(result);
                    }
                }
            } else {
                if (settings.addStatus) { result[settings.statusAttr] = 'UNCHANGED'; }
                if (settings.returnUnc) { newData.push(result); }
            }

            if (compareMap) { delete compareMap[id]; }
        }

        processed += results.length;
        offset += results.length;
    }

    if (settings.returnDel) {
        const values = Object.values(compareMap);
        for (const oldResult of values) {
            if (settings.addStatus) { oldResult[settings.statusAttr] = 'DELETED'; }
            newData.push(oldResult);
        }
    }

    console.log('Comparison finished');
    return newData;
}

function getChangeAttributes(obj1, obj2, prefix, out) {
    const changes = out ? out : [];
    if (obj1) {
        for (const key in obj1) {
            const v1 = obj1[key];
            const v2 = obj2 ? obj2[key] : null;
            if (!_.isEqual(v1, v2)) {
                if (v1 !== null && typeof v1 === 'object') {
                    getChangeAttributes(v1, v2, key + '/', changes);
                } else {
                    changes.push(prefix ? prefix + key : key);
                }
            }
        }
    }
    return changes;
}

Apify.main(async () => {
    const input = await Apify.getValue('INPUT');

    const actorId = input.actorId;
    if (!actorId) {
        throw new Error('Missing "actorId" in input');
    }

    const apifyClient = new Apify.ApifyClient();
    const runs = await apifyClient.acts.listRuns({
        actId: actorId,
        desc: true
    });

    const datasetIds = runs.items.slice(0, 2).map(run => run.defaultDatasetId);

    const resultData = await compareDatasets(datasetIds[1], datasetIds[0], input.idAttr, input.settings);

    if (resultData.length > 0) {
        await Apify.setValue('OUTPUT', resultData);
    }

    console.log('Finished comparing datasets');
});
