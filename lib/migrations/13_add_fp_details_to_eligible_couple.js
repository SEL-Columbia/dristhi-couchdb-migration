var FPDetailsMigration = function (_, q, DB_SERVER, DB_PORT, FORM_DB_NAME, DRISTHI_DB_NAME) {
    var openECs;
    var ecRegistrationFormSubmissions;
    var fpChangeFormSubmissions;
    var ppFPFormSubmissions;
    var fpFollowUpFormSubmissions;
    var renewFPProductFormSubmissions;
    var formRepository;
    var dristhiRepository;
    var allIUDFPDetails = [];
    var allCondomFPDetails = [];
    var allOCPFPDetails = [];
    var allFemaleSterilizationFPDetails = [];
    var allMaleSterilizationFPDetails = [];
    var allFemaleSterilizationFollowUpDates = [];
    var allMaleSterilizationFollowUpDates = [];
    var allOCPRefills = [];
    var allCondomRefills = [];
    var ecRegistrationFPDetailsMap = {
        fpMethod: 'currentMethod',
        fpAcceptanceDate: 'familyPlanningMethodChangeDate',
        lmpDate: 'lmpDate',
        uptResult: 'uptResult',
        iudPlace: 'iudPlace',
        femaleSterilizationType: 'femaleSterilizationType',
        maleSterilizationType: 'maleSterilizationType',
        numberOfOCPDelivered: 'numberOfOCPDelivered',
        numberOfCondomsSupplied: 'numberOfCondomsSupplied',
        entityId: 'id',
        date: 'registrationDate'
    };
    var fpChangeFPDetailsMap = {
        fpMethod: 'newMethod',
        fpAcceptanceDate: 'familyPlanningMethodChangeDate',
        lmpDate: 'lmpDate',
        uptResult: 'uptResult',
        iudPlace: 'iudPlace',
        femaleSterilizationType: 'femaleSterilizationType',
        maleSterilizationType: 'maleSterilizationType',
        numberOfOCPDelivered: 'numberOfOCPDelivered',
        numberOfCondomsSupplied: 'numberOfCondomsSupplied',
        entityId: 'id',
        date: 'familyPlanningMethodChangeDate'
    };
    var ppFPDetailsMap = {
        isFPPostDelivery: 'isFPPostDelivery',
        fpMethod: 'currentMethod',
        fpAcceptanceDate: 'familyPlanningMethodChangeDate',
        iudPlace: 'iudPlace',
        femaleSterilizationType: 'femaleSterilizationType',
        maleSterilizationType: 'maleSterilizationType',
        numberOfOCPDelivered: 'numberOfOCPDelivered',
        numberOfCondomsSupplied: 'numberOfCondomsSupplied',
        entityId: 'ecId',
        date: 'familyPlanningMethodChangeDate'
    };
    var fpFollowDetailsMap = {
        fpMethod: 'currentMethod',
        fpFollowupDate: 'fpFollowupDate',
        entityId: 'id'
    };
    var renewFPProductDetailsMap = {
        fpMethod: 'currentMethod',
        fpRenewMethodVisitDate: 'fpRenewMethodVisitDate',
        wasFPMethodRenewed: 'wasFPMethodRenewed',
        numberOfOCPDelivered: 'numberOfOCPDelivered',
        numberOfCondomsSupplied: 'numberOfCondomsSupplied',
        entityId: 'id'
    };

    var connectToDB = function () {
        var deferred = q.defer();
        var Repository = require('./../repository');
        formRepository = new Repository(DB_SERVER, DB_PORT, FORM_DB_NAME);
        dristhiRepository = new Repository(DB_SERVER, DB_PORT, DRISTHI_DB_NAME);
        deferred.resolve();
        return deferred.promise;
    };

    var createAllOpenECsView = function () {
        return dristhiRepository.createView('_design/EC_Temp', {
            allOpenECs: {
                map: function (doc) {
                    if (doc.type === 'EligibleCouple' &&
                        doc.isClosed === 'false' && doc.isOutOfArea === 'false') {
                        emit(doc._id, doc.caseId);
                    }
                }
            }
        });
    };

    var getAllOpenECs = function () {
        var deferred = q.defer();
        dristhiRepository
            .queryView('EC_Temp/allOpenECs', {
                include_docs: true
            })
            .then(function (response) {
                openECs = response;
                console.log('Found ' + response.length + ' open ECs.');
                deferred.resolve();
            }, function (error) {
                deferred.reject(error);
            });
        return deferred.promise;
    };

    var createECRegistrationFormSubmissionsView = function () {
        return formRepository.createView('_design/ECRegistration_FormSubmission_Temp', {
            byFormName: {
                map: function (doc) {
                    if (doc.type === 'FormSubmission' && doc.formName === 'ec_registration' && doc.entityId) {
                        emit(doc.entityId);
                    }
                }
            }
        });
    };

    var createFPChangeFormSubmissionsView = function () {
        return formRepository.createView('_design/FPChange_FormSubmission_Temp', {
            byFormName: {
                map: function (doc) {
                    if (doc.type === 'FormSubmission' && doc.formName === 'fp_change' && doc.entityId) {
                        emit(doc.entityId);
                    }
                }
            }
        });
    };

    var createPPFPFormSubmissionsView = function () {
        return formRepository.createView('_design/PPFP_FormSubmission_Temp', {
            byFormName: {
                map: function (doc) {
                    var ecId = doc.formInstance.form.fields.filter(function (field) {
                        return field.name === 'ecId';
                    })[0].value;
                    if (doc.type === 'FormSubmission' && doc.formName === 'postpartum_family_planning' && ecId) {
                        emit(ecId);
                    }
                }
            }
        });
    };

    var createFPFollowUpSubmissionsView = function () {
        return formRepository.createView('_design/FPFollowUp_FormSubmission_Temp', {
            byFormName: {
                map: function (doc) {
                    var fpMethod = doc.formInstance.form.fields.filter(function (field) {
                        return field.name === 'currentMethod';
                    })[0].value;
                    if (doc.type === 'FormSubmission' && doc.formName === 'fp_followup' && doc.entityId &&
                        (fpMethod === 'female_sterilization' || fpMethod === 'male_sterilization')) {
                        emit(doc.entityId);
                    }
                }
            }
        });
    };

    var createRenewFPProductSubmissionsView = function () {
        return formRepository.createView('_design/RenewFPProduct_FormSubmission_Temp', {
            byFormName: {
                map: function (doc) {
                    if (doc.type === 'FormSubmission' && doc.formName === 'renew_fp_product' && doc.entityId) {
                        emit(doc.entityId);
                    }
                }
            }
        });
    };

    var getAllECRegistrations = function () {
        var deferred = q.defer();
        var ecIds = _.pluck(openECs, 'value');

        formRepository.queryView('ECRegistration_FormSubmission_Temp/byFormName', {
            keys: ecIds,
            include_docs: true
        }).then(function (response) {
            ecRegistrationFormSubmissions = response;
            console.log('Found %s EC Registration Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getAllFPChanges = function () {
        var deferred = q.defer();
        var ecIds = _.pluck(openECs, 'value');

        formRepository.queryView('FPChange_FormSubmission_Temp/byFormName', {
            keys: ecIds,
            include_docs: true
        }).then(function (response) {
            fpChangeFormSubmissions = response;
            console.log('Found %s FP Change Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getAllPPFPs = function () {
        var deferred = q.defer();
        var ecIds = _.pluck(openECs, 'value');

        formRepository.queryView('PPFP_FormSubmission_Temp/byFormName', {
            keys: ecIds,
            include_docs: true
        }).then(function (response) {
            ppFPFormSubmissions = response;
            console.log('Found %s PPFP Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getAllFPFollowUps = function () {
        var deferred = q.defer();
        var ecIds = _.pluck(openECs, 'value');

        formRepository.queryView('FPFollowUp_FormSubmission_Temp/byFormName', {
            keys: ecIds,
            include_docs: true
        }).then(function (response) {
            fpFollowUpFormSubmissions = response;
            console.log('Found %s FP FollowUp Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getAllRenewFPProducts = function () {
        var deferred = q.defer();
        var ecIds = _.pluck(openECs, 'value');

        formRepository.queryView('RenewFPProduct_FormSubmission_Temp/byFormName', {
            keys: ecIds,
            include_docs: true
        }).then(function (response) {
            renewFPProductFormSubmissions = response;
            console.log('Found %s Renew FP Product Form Submissions.', response.length);
            deferred.resolve();
        }, function (error) {
            deferred.reject(error);
        });
        return deferred.promise;
    };

    var getRefillableFPDetails = function (entityId, fpAcceptanceDate, quantity) {
        var refillableFPDetails = {};
        refillableFPDetails['fpAcceptanceDate'] = fpAcceptanceDate;
        refillableFPDetails['entityId'] = entityId;
        var refills = [];
        if (quantity !== undefined) {
            refills.push({"date": fpAcceptanceDate, "quantity": quantity});
        }
        refillableFPDetails['refills'] = refills;
        return refillableFPDetails;
    };

    var getIUDFPDetails = function (entityId, fpAcceptanceDate, iudPlaceValue) {
        var iudFPDetails = {};
        iudFPDetails['fpAcceptanceDate'] = fpAcceptanceDate;
        iudFPDetails['entityId'] = entityId;

        iudFPDetails['iudPlace'] = iudPlaceValue !== undefined ? iudPlaceValue : '';

        return iudFPDetails;
    };

    var getSterilizationFPDetails = function (entityId, fpAcceptanceDate, sterilizationType) {
        var sterilizationFPDetails = {};
        sterilizationFPDetails['entityId'] = entityId;
        if (sterilizationType !== undefined) {
            sterilizationFPDetails['typeOfSterilization'] = sterilizationType;
        }
        sterilizationFPDetails['sterilizationDate'] = fpAcceptanceDate;
        sterilizationFPDetails['followupVisitDates'] = [];
        return sterilizationFPDetails;
    };

    var getFPDetails = function (fpDetailsMap) {
        if (fpDetailsMap.fpMethod === 'ocp') {
            fpDetailsMap.ocpFPDetails = getRefillableFPDetails(fpDetailsMap.entityId, fpDetailsMap.fpAcceptanceDate, fpDetailsMap.numberOfOCPDelivered);
            fpDetailsMap.ocpFPDetails.lmpDate = fpDetailsMap.lmpDate;
            fpDetailsMap.ocpFPDetails.uptResult = fpDetailsMap.uptResult;
        }

        if (fpDetailsMap.fpMethod === 'condom') {
            fpDetailsMap.condomFPDetails = getRefillableFPDetails(fpDetailsMap.entityId, fpDetailsMap.fpAcceptanceDate, fpDetailsMap.numberOfCondomsSupplied);
        }

        if (fpDetailsMap.fpMethod === 'iud') {
            fpDetailsMap.iudFPDetails = getIUDFPDetails(fpDetailsMap.entityId, fpDetailsMap.fpAcceptanceDate, fpDetailsMap.iudPlace);
            fpDetailsMap.iudFPDetails.lmpDate = fpDetailsMap.lmpDate;
            fpDetailsMap.iudFPDetails.uptResult = fpDetailsMap.uptResult;
        }

        if (fpDetailsMap.fpMethod === 'female_sterilization') {
            fpDetailsMap.femaleSterilizationFPDetails = getSterilizationFPDetails(fpDetailsMap.entityId, fpDetailsMap.fpAcceptanceDate, fpDetailsMap.femaleSterilizationType);
        }

        if (fpDetailsMap.fpMethod === 'male_sterilization') {
            fpDetailsMap.maleSterilizationFPDetails = getSterilizationFPDetails(fpDetailsMap.entityId, fpDetailsMap.fpAcceptanceDate, fpDetailsMap.maleSterilizationType);
        }
        return fpDetailsMap;
    };

    var getFPDetailsAndEntityIdsFromFPChange = function () {
        var deferred = q.defer();

        var fpDetails = getFPDetailsFromForm(fpChangeFormSubmissions, fpChangeFPDetailsMap);
        allIUDFPDetails = allIUDFPDetails.concat(fpDetails.iudFPDetails);
        allCondomFPDetails = allCondomFPDetails.concat(fpDetails.condomFPDetails);
        allOCPFPDetails = allOCPFPDetails.concat(fpDetails.ocpFPDetails);
        allFemaleSterilizationFPDetails = allFemaleSterilizationFPDetails.concat(fpDetails.femaleSterilizationFPDetails);
        allMaleSterilizationFPDetails = allMaleSterilizationFPDetails.concat(fpDetails.maleSterilizationFPDetails);

        log("FP Change", fpDetails.iudFPDetails, fpDetails.condomFPDetails, fpDetails.ocpFPDetails, fpDetails.femaleSterilizationFPDetails, fpDetails.maleSterilizationFPDetails);

        deferred.resolve();
        return deferred.promise;
    };

    var getFPDetailsAndEntityIdsFromPPFP = function () {
        var deferred = q.defer();

        var fpDetails = getFPDetailsFromForm(ppFPFormSubmissions, ppFPDetailsMap);
        allIUDFPDetails = allIUDFPDetails.concat(fpDetails.iudFPDetails);
        allCondomFPDetails = allCondomFPDetails.concat(fpDetails.condomFPDetails);
        allOCPFPDetails = allOCPFPDetails.concat(fpDetails.ocpFPDetails);
        allFemaleSterilizationFPDetails = allFemaleSterilizationFPDetails.concat(fpDetails.femaleSterilizationFPDetails);
        allMaleSterilizationFPDetails = allMaleSterilizationFPDetails.concat(fpDetails.maleSterilizationFPDetails);

        log("PP FP form", fpDetails.iudFPDetails, fpDetails.condomFPDetails, fpDetails.ocpFPDetails, fpDetails.femaleSterilizationFPDetails, fpDetails.maleSterilizationFPDetails);

        deferred.resolve();
        return deferred.promise;
    };

    var getFollowUpDetailsFromForm = function (formSubmissions, fpMap) {
        var fpDetails = _.map(formSubmissions, function (formSubmission) {
            var fpDetailsMap = getDetailsFromForm(fpMap, formSubmission);
            return fpDetailsMap;

        });
        return {
            allMaleSterilizationFollowUpDates: _.filter(fpDetails, function (fpDetail) {
                return fpDetail.fpMethod === 'male_sterilization';
            }),
            allFemaleSterilizationFollowUpDates: _.filter(fpDetails, function (fpDetail) {
                return fpDetail.fpMethod === 'female_sterilization';
            })
        }
    };

    var getRefillDetailsFromForm = function (formSubmissions, fpMap) {
        var fpDetails = _.map(formSubmissions, function (formSubmission) {
            var fpDetailsMap = getDetailsFromForm(fpMap, formSubmission);
            var refill = {};
            refill['date'] = fpDetailsMap.fpRenewMethodVisitDate;
            if (fpDetailsMap.fpMethod === 'ocp' && fpDetailsMap.wasFPMethodRenewed) {
                refill['quantity'] = fpDetailsMap.numberOfOCPDelivered;
            }
            else if (fpDetailsMap.fpMethod === 'condom' && fpDetailsMap.wasFPMethodRenewed) {
                refill['quantity'] = fpDetailsMap.numberOfCondomsSupplied;
            }
            fpDetailsMap.refill = refill;
            return fpDetailsMap;
        });
        return {
            allOCPRefills: _.filter(fpDetails, function (fpDetail) {
                return fpDetail.fpMethod === 'ocp';
            }),
            allCondomRefills: _.filter(fpDetails, function (fpDetail) {
                return fpDetail.fpMethod === 'condom';
            })
        }
    };

    var getFPDetailsAndEntityIdsFromFPFollowUp = function () {
        var deferred = q.defer();
        var fpDetails = getFollowUpDetailsFromForm(fpFollowUpFormSubmissions, fpFollowDetailsMap);
        allMaleSterilizationFollowUpDates = fpDetails.allMaleSterilizationFollowUpDates;
        allFemaleSterilizationFollowUpDates = fpDetails.allFemaleSterilizationFollowUpDates;

        console.log("FP followup: ");
        console.log("Male Sterilization followup dates: " + JSON.stringify(fpDetails.allMaleSterilizationFollowUpDates));
        console.log("Female Sterilization followup dates: " + JSON.stringify((fpDetails.allFemaleSterilizationFollowUpDates)));

        deferred.resolve();
        return deferred.promise;
    };

    var getFPDetailsAndEntityIdsFromRenewFPProduct = function () {
        var deferred = q.defer();
        var fpDetails = getRefillDetailsFromForm(renewFPProductFormSubmissions, renewFPProductDetailsMap);
        allOCPRefills = fpDetails.allOCPRefills;
        allCondomRefills = fpDetails.allCondomRefills;

        console.log("Renew FP Product: ");
        console.log("OCP Refills:  " + JSON.stringify(fpDetails.allOCPRefills));
        console.log("Condom Refills: " + JSON.stringify(fpDetails.allCondomRefills));
        deferred.resolve();
        return deferred.promise;
    };

    var getDetailsFromForm = function (fpFieldsMap, formSubmission) {
        var fpDetailsMap = {};
        for (var fpField in fpFieldsMap) {
            if (fpFieldsMap.hasOwnProperty(fpField)) {
                fpDetailsMap[fpField] = _.find(formSubmission.doc.formInstance.form.fields,function (field) {
                    return field.name === fpFieldsMap[fpField];
                }).value;
            }
        }
        return fpDetailsMap;
    };

    var getFPDetailsFromForm = function (formSubmissions, fpMap) {
        var fpDetails = _.map(formSubmissions, function (formSubmission) {
            var fpDetailsMap = getDetailsFromForm(fpMap, formSubmission);
            fpDetailsMap = getFPDetails(fpDetailsMap);
            return fpDetailsMap;
        });
        return {
            iudFPDetails: _.pluck(_.where(fpDetails, {'fpMethod': 'iud'}), 'iudFPDetails'),
            condomFPDetails: _.pluck(_.where(fpDetails, {'fpMethod': 'condom'}), 'condomFPDetails'),
            ocpFPDetails: _.pluck(_.where(fpDetails, {'fpMethod': 'ocp'}), 'ocpFPDetails'),
            femaleSterilizationFPDetails: _.pluck(_.where(fpDetails, {'fpMethod': 'female_sterilization'}), 'femaleSterilizationFPDetails'),
            maleSterilizationFPDetails: _.pluck(_.where(fpDetails, {'fpMethod': 'male_sterilization'}), 'maleSterilizationFPDetails')
        };
    };

    var log = function (form, allIUDFPDetails, allCondomFPDetails, allOCPFPDetails, allFemaleSterilizationFPDetails, allMaleSterilizationFPDetails) {
        console.log(form + ": ");
        console.log("IUD Details : " + JSON.stringify(allIUDFPDetails));
        console.log("Condom Details : " + JSON.stringify(allCondomFPDetails));
        console.log("OCP Details : " + JSON.stringify(allOCPFPDetails));
        console.log("Female Sterilization Details : " + JSON.stringify(allFemaleSterilizationFPDetails));
        console.log("Male Sterilization Details : " + JSON.stringify(allMaleSterilizationFPDetails));
    };

    var getFPDetailsAndEntityIdsFromECRegistration = function () {
        var deferred = q.defer();
        var fpDetails = getFPDetailsFromForm(ecRegistrationFormSubmissions, ecRegistrationFPDetailsMap);
        allIUDFPDetails = fpDetails.iudFPDetails;
        allCondomFPDetails = fpDetails.condomFPDetails;
        allOCPFPDetails = fpDetails.ocpFPDetails;
        allFemaleSterilizationFPDetails = fpDetails.femaleSterilizationFPDetails;
        allMaleSterilizationFPDetails = fpDetails.maleSterilizationFPDetails;
        log("EC Registration", allIUDFPDetails, allCondomFPDetails, allOCPFPDetails, allFemaleSterilizationFPDetails, allMaleSterilizationFPDetails);
        deferred.resolve();
        return deferred.promise;
    };

    var fetchFPDetailsForThisEC = function (allGivenFPDetails, ec) {
        var allGivenFPDetailsForEC = _.where(allGivenFPDetails, {entityId: ec.value});
        _.each(allGivenFPDetailsForEC, function (givenFPDetail) {
            delete givenFPDetail.entityId;
        });
        return allGivenFPDetailsForEC;
    };

    var fetchFollowUpDetailsForThisEC = function (allFollowUpDates, ec) {
        var allFollowUpDatesForEC = _.where(allFollowUpDates, {entityId: ec.value});
        var folllowupVisitDates = [];
        _.each(allFollowUpDatesForEC, function (givenFollowUpDetail) {
            folllowupVisitDates.push(givenFollowUpDetail.fpFollowupDate);
        });
        return folllowupVisitDates;
    };

    var fetchRefillDetailsForThisEC = function (allRefillDetails, ec) {
        var allRefillsForThisEC = _.where(allRefillDetails, {entityId: ec.value});
        var refills = [];
        _.each(allRefillsForThisEC, function (givenRefill) {
            refills.push({date: givenRefill.refill.date, quantity: givenRefill.refill.quantity});
        });
        return refills;
    };

    var updateECWithIUDDetails = function () {
        var deferred = q.defer();
        console.log("IUD Details: " + JSON.stringify(allIUDFPDetails));

        _.each(openECs, function (ec) {
            ec.doc.iudFPDetails = fetchFPDetailsForThisEC(allIUDFPDetails, ec);
        });
        deferred.resolve();
        return deferred.promise;
    };

    var getAppropriateFPForWhichThisRefillHappened = function (refill, multipleFPDetails) {
        return _.last(_.filter(multipleFPDetails, function (eachFPDetail) {
            return new Date(refill.date) - new Date(eachFPDetail.fpAcceptanceDate) > 0;
        }));
    };

    var getAppropriateFPForWhichThisFollowupHappened = function (followupVisitDate, multipleFPDetails) {
        return _.last(_.filter(multipleFPDetails, function (eachFPDetail) {
            return (new Date(new String(followupVisitDate)) - new Date(new String(eachFPDetail.sterilizationDate))) > 0;
        }));
    };

    var updateCondomFPDetailsWithRefills = function (refills, condomFPDetails) {
        _.each(refills, function (refill) {
            var fpDetails = getAppropriateFPForWhichThisRefillHappened(refill, condomFPDetails);
            if (fpDetails !== undefined) {
                var index = _.indexOf(condomFPDetails, fpDetails);
                condomFPDetails[index].refills = condomFPDetails[index].refills.concat(refill);
            }
        });
        return condomFPDetails;
    };

    var updateOCPFPDetailsWithRefills = function (refills, ocpFPDetails) {
        _.each(refills, function (refill) {
            var fpDetails = getAppropriateFPForWhichThisRefillHappened(refill, ocpFPDetails);
            if (fpDetails !== undefined) {
                var index = _.indexOf(ocpFPDetails, fpDetails);
                ocpFPDetails[index].refills = ocpFPDetails[index].refills.concat(refill);
            }
        });
        return ocpFPDetails;
    };

    var updateFemaleSterilizationFPDetailsWithFollowupVisitDates = function (followupVisitDates, femaleSterilizationFPDetails) {
        _.each(followupVisitDates, function (followupVisitDate) {
            var fpDetails =
                getAppropriateFPForWhichThisFollowupHappened(followupVisitDate, femaleSterilizationFPDetails);
            if (fpDetails !== undefined) {
                var index = _.indexOf(femaleSterilizationFPDetails, fpDetails);
                femaleSterilizationFPDetails[index].followupVisitDates =
                    femaleSterilizationFPDetails[index].followupVisitDates.concat(followupVisitDate);
            }
        });
        return femaleSterilizationFPDetails;
    };

    var updateMaleSterilizationFPDetailsWithFollowupVisitDates = function (followupVisitDates, maleSterilizationFPDetails) {
        _.each(followupVisitDates, function (followupVisitDate) {
            var fpDetails = getAppropriateFPForWhichThisFollowupHappened(followupVisitDate, maleSterilizationFPDetails);
            if (fpDetails !== undefined) {
                var index = _.indexOf(maleSterilizationFPDetails, fpDetails);
                maleSterilizationFPDetails[index].followupVisitDates =
                    maleSterilizationFPDetails[index].followupVisitDates.concat(followupVisitDate);
            }
        });
        return maleSterilizationFPDetails;
    };

    var updateECWithCondomDetails = function () {
        var deferred = q.defer();
        console.log("Condom Details: " + JSON.stringify(allCondomFPDetails));
        console.log("Condom Refill Details: " + JSON.stringify(allCondomRefills));
        _.each(openECs, function (ec) {
            ec.doc.condomFPDetails = fetchFPDetailsForThisEC(allCondomFPDetails, ec);
            var refills = fetchRefillDetailsForThisEC(allCondomRefills, ec);
            ec.doc.condomFPDetails = updateCondomFPDetailsWithRefills(refills, ec.doc.condomFPDetails);
        });

        deferred.resolve();
        return deferred.promise;
    };

    var updateECWithOCPFPDetails = function () {
        var deferred = q.defer();
        console.log("OCP Details: " + JSON.stringify(allOCPFPDetails));
        console.log("OCP Refill Details: " + JSON.stringify(allOCPRefills));
        _.each(openECs, function (ec) {
            ec.doc.ocpFPDetails = fetchFPDetailsForThisEC(allOCPFPDetails, ec);
            var refills = fetchRefillDetailsForThisEC(allOCPRefills, ec);
            ec.doc.ocpFPDetails = updateOCPFPDetailsWithRefills(refills, ec.doc.ocpFPDetails);
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateECWithFemaleSterilizationFPDetails = function () {
        var deferred = q.defer();
        console.log("Female Sterilization Details: " + JSON.stringify(allFemaleSterilizationFPDetails));
        console.log("Female Sterilization Followup Details: " + JSON.stringify(allFemaleSterilizationFollowUpDates));
        _.each(openECs, function (ec) {
            ec.doc.femaleSterilizationFPDetails = fetchFPDetailsForThisEC(allFemaleSterilizationFPDetails, ec);
            var followupVisitDates = fetchFollowUpDetailsForThisEC(allFemaleSterilizationFollowUpDates, ec);
            if (followupVisitDates !== undefined) {
                ec.doc.femaleSterilizationFPDetails =
                    updateFemaleSterilizationFPDetailsWithFollowupVisitDates(followupVisitDates, ec.doc.femaleSterilizationFPDetails);
            }
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateECWithMaleSterilizationFPDetails = function () {
        var deferred = q.defer();
        console.log("Male Sterilization Details: " + JSON.stringify(allMaleSterilizationFPDetails));
        console.log("Male Sterilization Followup Details: " + JSON.stringify(allMaleSterilizationFollowUpDates));
        _.each(openECs, function (ec) {
            ec.doc.maleSterilizationFPDetails = fetchFPDetailsForThisEC(allMaleSterilizationFPDetails, ec);
            var followupVisitDates = fetchFollowUpDetailsForThisEC(allMaleSterilizationFollowUpDates, ec);
            ec.doc.maleSterilizationFPDetails =
                updateMaleSterilizationFPDetailsWithFollowupVisitDates(followupVisitDates, ec.doc.maleSterilizationFPDetails);
        });
        deferred.resolve();
        return deferred.promise;
    };

    var updateECDocument = function () {
        var deferred = q.defer();
        var ECs = _.map(openECs, function (ec) {
            return ec.doc;
        });
        dristhiRepository.save(ECs);
        console.log("ECS: " + JSON.stringify(ECs));
        deferred.resolve();
        return deferred.promise;
    };

    var deleteEC_TempView = function () {
        return dristhiRepository.deleteDocById('_design/EC_Temp');
    };

    var deleteECRegistrationFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/ECRegistration_FormSubmission_Temp');
    };

    var deleteFPChangeFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FPChange_FormSubmission_Temp');
    };
    var deletePPFPFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/PPFP_FormSubmission_Temp');
    };

    var deleteFPFollowUpFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FPFollowUp_FormSubmission_Temp');
    };

    var deleteFPReferralFollowUpFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/FPReferralFollowUp_FormSubmission_Temp');
    };

    var deleteRenewFPProductFormSubmission_TempView = function () {
        return formRepository.deleteDocById('_design/RenewFPProduct_FormSubmission_Temp');
    };

    var cleanupViewsAndCompactDB = function () {
        return q.all([
            deleteEC_TempView(),
            deleteECRegistrationFormSubmission_TempView(),
            deleteFPChangeFormSubmission_TempView(),
            deletePPFPFormSubmission_TempView(),
            deleteFPFollowUpFormSubmission_TempView(),
            deleteFPReferralFollowUpFormSubmission_TempView(),
            deleteRenewFPProductFormSubmission_TempView(),
        ]);
    };

    var reportMigrationComplete = function () {
        var deferred = q.defer();
        deferred.resolve();
        console.log("Migration complete.");
        return deferred.promise;
    };

    var reportMigrationFailure = function (err) {
        var deferred = q.defer();
        deferred.resolve();
        console.error("Migration Failed. Error: %s.", JSON.stringify(err));
        return deferred.promise;
    };

    var migrate = function () {
        connectToDB()
            .then(createECRegistrationFormSubmissionsView)
            .then(createFPChangeFormSubmissionsView)
            .then(createPPFPFormSubmissionsView)
            .then(createFPFollowUpSubmissionsView)
            .then(createRenewFPProductSubmissionsView)
            .then(createAllOpenECsView)
            .then(getAllOpenECs)
            .then(getAllECRegistrations)
            .then(getFPDetailsAndEntityIdsFromECRegistration)
            .then(getAllFPChanges)
            .then(getFPDetailsAndEntityIdsFromFPChange)
            .then(getAllPPFPs)
            .then(getFPDetailsAndEntityIdsFromPPFP)
            .then(getAllFPFollowUps)
            .then(getFPDetailsAndEntityIdsFromFPFollowUp)
            .then(getAllRenewFPProducts)
            .then(getFPDetailsAndEntityIdsFromRenewFPProduct)
            .then(updateECWithIUDDetails)
            .then(updateECWithCondomDetails)
            .then(updateECWithOCPFPDetails)
            .then(updateECWithFemaleSterilizationFPDetails)
            .then(updateECWithMaleSterilizationFPDetails)
            .then(updateECDocument)
            .then(reportMigrationComplete, reportMigrationFailure)
            .fin(cleanupViewsAndCompactDB);
    };

    return {
        migrate: migrate
    };
};

module.exports = FPDetailsMigration;