var _ = require('underscore')._;
var cradle = require('cradle');
var ancs = require('./ancs.js')
var ancVisits = [];

function ANCVisit(entityId, ancVisitNumber, ancVisitDate, weight, bpSystolic, bpDiastolic) {

    'use strict';
    var self = this;
    self.entityId = entityId;
    self.ancVisitNumber = ancVisitNumber;
    self.ancVisitDate = ancVisitDate;
    self.weight = weight;
    self.bpSystolic = bpSystolic;
    self.bpDiastolic = bpDiastolic;
}

var dristhiFormDb = new (cradle.Connection)(('http://localhost', 5984, {
    cache: true,
    raw: false
})).database('drishti-form');


dristhiFormDb.exists(function (err, exists) {
    if (err) {
        console.log('error', err);
    } else if (exists) {
        console.log('the force is with you.');
    } else {
        console.log('database does not exists.');
        dristhiFormDb.create();
        /* populate design documents */
    }
});
dristhiFormDb.save('_design/FormSubmissions', {
    views: {
        byFormName: {
            map: function (doc) {
                if (doc.formName === 'anc_visit') {
                    emit(doc, null);
                }
            }
        }
    }
});

dristhiFormDb.view('FormSubmissions/byFormName'
    , function (err, res) {
        res.forEach(function (row) {
            var entityId = row.key.entityId;
            var ancVisitDate = _.find(row.key.formInstance.form.fields, function (field) {
                return field.name == 'ancVisitDate';
            });
            var ancVisitNumber = _.find(row.key.formInstance.form.fields, function (field) {
                return field.name == 'ancVisitNumber';
            });
            var weight = _.find(row.key.formInstance.form.fields, function (field) {
                return field.name == 'weight';
            });
            var bpSystolic = _.find(row.key.formInstance.form.fields, function (field) {
                return field.name == 'bpSystolic';
            });
            var bpDiastolic = _.find(row.key.formInstance.form.fields, function (field) {
                return field.name == 'bpDiastolic';
            });
//        console.log("Entity Id : %s, ANC Visit Number : %s, ANC Visit Date : %s, Weight : %s, BP : %s/%s "
//            , entityId, ancVisitNumber.value, ancVisitDate.value, weight.value, bpSystolic.value, bpDiastolic.value);

            ancVisits.push(new ANCVisit(entityId, ancVisitNumber.value, ancVisitDate.value, weight.value, bpSystolic.value, bpDiastolic.value));
        });
//        console.log("ANC VISITS: "+JSON.stringify(ancVisits));
        var ecIds = _.pluck(ancVisits, 'entityId');
        var openANCs = ancs.getANC(ecIds, ancVisits);
//    ancs.filterByEcIds(ecIds, openANCs);
    });


//requiredANCs.forEach(function(anc) {
//    anc.key.ancVisits = _.filter(ancVisits, function (ancVisit) {
//        return anc.key.caseId === ancVisit.entityId;
//    });
//    anc.key.ancVisits.forEach(function (ancVisit){ delete ancVisit.entityId; });
//});







