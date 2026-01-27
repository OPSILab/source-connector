const express = require('express');
const axios = require('axios');
const logger = require('percocologger')
const config = require('../config.js');

function getEndpointVersionApi(subId) {
    return (config.orion.apiVersion == "v2" || (subId && !subId.startsWith("urn:ngsi-ld:Subscription:")) ? "/v2/subscriptions" : "/ngsi-ld/v1/subscriptions")
}

async function createOrionSubscription({
    orionBaseUrl,
    notificationUrl,
    fiwareService,
    fiwareServicePath
}) {
    if (await checkMultipleSubscriptions(notificationUrl) > 0)
        return logger.warn(message = "Already existing subscription found for the same notification URL.") || message;
    const sub = config.orion.apiVersion == "v2" ?
        {
            description: `Query engine subscription`,
            subject: {
                entities: [{ idPattern: '.*' }],
            },

            notification: {
                http: { url: notificationUrl },
                attrs: [],
            },
            throttling: 1
        } :
        {
            type: "Subscription",
            entities: [
                {
                    type: config.orion.subscribeType || "Thing"
                }
            ],
            watchedAttributes: config.orion.watchedAttributes || [config.orion.attrWithUrl],
            notification: {
                endpoint: {
                    uri: config.orion.notificationUrl,
                    accept: "application/json"
                }
            },
            throttling: 5,
            expires: new Date(new Date().getTime() + 365 * 24 * 60 * 60 * 1000).toISOString(),
        }

    const headers = { 'Content-Type': 'application/json' };
    if (fiwareService) headers['Fiware-Service'] = fiwareService;
    if (fiwareServicePath) headers['Fiware-ServicePath'] = fiwareServicePath;

    const url = `${orionBaseUrl.replace(/\/$/, '')}${getEndpointVersionApi()}`;
    logger.info(url, sub, { headers })
    const res = await axios.post(url, sub, { headers });
    logger.info({ status: res.status })
    return res.data;
}

if (config.orion.subscribe)
    createOrionSubscription({
        orionBaseUrl: config.orion?.orionBaseUrl || 'http://localhost:1027',
        notificationUrl: config.orion?.notificationUrl || 'http://host.docker.internal:3000/api/orion/subscribe',
        fiwareService: config.orion?.fiwareService,
        fiwareServicePath: config.orion?.fiwareServicePath
    }).then(sub => {
        if (sub != "Already existing subscription found for the same notification URL.")
            logger.info("Orion subscription created: " + sub)
    }).catch(err => {
        logger.error("Error creating Orion subscription: ", err.response?.data || err.message || err)
        err.response?.config?.data && logger.error(err.response?.config?.data)
    })

async function getSubscriptions() {
    return (await axios.get((config.orion.orionBaseUrl || 'http://localhost:1027') + getEndpointVersionApi(),
        (config?.orion?.fiwareService ?
            {
                headers:
                {
                    'Fiware-Service': config.orion.fiwareService || 'service',
                    'Fiware-ServicePath': config.orion.fiwareServicePath || '/service'
                }
            }
            :
            {}
        ))).data
}

async function deleteSubscription(subId) {
    return (await axios.delete(`${(config.orion.orionBaseUrl || 'http://localhost:1027')}${getEndpointVersionApi(subId)}/${subId}`, (config?.orion?.fiwareService ?
            {
                headers:
                {
                    'Fiware-Service': config.orion.fiwareService || 'service',
                    'Fiware-ServicePath': config.orion.fiwareServicePath || '/service'
                }
            }
            :
            {}
        ))).data
}

function typesCheck(subTypes) {
    // subTypes?.[0]?.type == (config.orion.subscribeType || "Thing")
    return subTypes.find(subType => subType.type === (config.orion.subscribeType || "Thing"))
}

function attributesCheck(subAttributes) {
    const sortedActuallyWatchedAttributes = [...subAttributes].sort();
    const sortedDesiredWatchedAttributes = [...config.orion.watchedAttributes].sort();

    if (sortedActuallyWatchedAttributes.length !== sortedDesiredWatchedAttributes.length) return false;
    return sortedActuallyWatchedAttributes.every((val, i) => val === sortedDesiredWatchedAttributes[i]);
}

async function checkMultipleSubscriptions(notificationUrl) {
    let subscriptions = await getSubscriptions()
    console.log(JSON.stringify(subscriptions, null, 2))
    let count = 0
    for (let sub of subscriptions) {
        if (config.orion.purgeSubscriptionsAtStart)
            await deleteSubscription(sub.id)
        else if (config.orion.deleteAllDuplicateSubscriptions && (sub.notification?.http?.url === notificationUrl || sub.notification?.endpoint?.uri === notificationUrl)) {
            if (count > 0) {
                console.log(`Deleting duplicate subscription with id ${sub.id}`)
                await deleteSubscription(sub.id)
            }
            else
                count++
        }
        else if (
            (sub.subject?.entities?.[0]?.idPattern === '.*' || (typesCheck(sub.entities) && attributesCheck(sub.watchedAttributes)))
            &&
            (sub.notification?.http?.url === notificationUrl || sub.notification?.endpoint?.uri === notificationUrl)
            &&
            (!sub.description || sub.description === `Query engine subscription`)
        ) {
            if (count > 0) {
                console.log(`Deleting duplicate subscription with id ${sub.id}`)
                await deleteSubscription(sub.id)
            }
            else
                count++
        }
    }
    return count;
}

module.exports = { createOrionSubscription };