#include "MELALinearInterpFunc.h"
#include "MELANCSplineFactory_1D.h"
#include "MELANCSplineFactory_2D.h"
#include "MELANCSplineFactory_3D.h"
#include "MELAFuncPdf.h"
#include "Mela.h"

#ifdef __CINT__


#pragma link off all globals;
#pragma link off all classes;
#pragma link off all functions;

#pragma link C++ nestedclasses;
#pragma link C++ nestedtypedefs;

#pragma link C++ class MELAParticle;
#pragma link C++ class std::vector<MELAParticle*>;
#pragma link C++ class MELATopCandidate;
#pragma link C++ class MELACandidate;
#pragma link C++ class MelaIO;
#pragma link C++ namespace TVar;

#pragma link C++ class MELAFuncPdf;
#pragma link C++ class MELALinearInterpFunc;
#pragma link C++ class MELANCSplineCore;
#pragma link C++ class MELANCSpline_1D_fast;
#pragma link C++ class MELANCSpline_2D_fast;
#pragma link C++ class MELANCSpline_3D_fast;
#pragma link C++ class MELANCSplineFactory_1D;
#pragma link C++ class MELANCSplineFactory_2D;
#pragma link C++ class MELANCSplineFactory_3D;

#pragma link C++ namespace TUtil;
#pragma link C++ function TUtil::computeAngles;
#pragma link C++ function TUtil::computeAnglesCS;
#pragma link C++ function TUtil::computeVBFAngles;
#pragma link C++ function TUtil::computeVBFAngles_ComplexBoost;
#pragma link C++ function TUtil::computeVHAngles;
#pragma link C++ function TUtil::scaleMomentumToEnergy;
#pragma link C++ function TUtil::constrainedRemovePairMass;
#pragma link C++ function TUtil::removeMassFromPair;
#pragma link C++ function TUtil::adjustTopDaughters;
#pragma link C++ function TUtil::computeFakeJet;
//
#pragma link C++ function TUtil::SetAlphaS;
#pragma link C++ function TUtil::CheckPartonMomFraction;
#pragma link C++ function TUtil::ComputePDF;
#pragma link C++ function TUtil::SumMEPDF;
//
#pragma link C++ function TUtil::GetBoostedParticleVectors;
#pragma link C++ function TUtil::ConvertVectorFormat;
#pragma link C++ function TUtil::ConvertTopCandidate;
//
#pragma link C++ function TUtil::SumMatrixElementPDF;
#pragma link C++ function TUtil::JHUGenMatEl;
#pragma link C++ function TUtil::HJJMatEl;
#pragma link C++ function TUtil::VHiggsMatEl;
#pragma link C++ function TUtil::TTHiggsMatEl;
#pragma link C++ function TUtil::BBHiggsMatEl;

#pragma link C++ class SpinZeroCouplings;
#pragma link C++ class SpinOneCouplings;
#pragma link C++ class SpinTwoCouplings;
#pragma link C++ class VprimeCouplings;
#pragma link C++ class TEvtProb;
#pragma link C++ class ZZMatrixElement;
#pragma link C++ class Mela;


#endif
// Do NOT change. Changes will be lost next time file is generated

#define R__DICTIONARY_FILENAME dOdIsrcdILinkDef_out

/*******************************************************************/
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#define G__DICTIONARY
#include "RConfig.h"
#include "TClass.h"
#include "TDictAttributeMap.h"
#include "TInterpreter.h"
#include "TROOT.h"
#include "TBuffer.h"
#include "TMemberInspector.h"
#include "TInterpreter.h"
#include "TVirtualMutex.h"
#include "TError.h"

#ifndef G__ROOT
#define G__ROOT
#endif

#include "RtypesImp.h"
#include "TIsAProxy.h"
#include "TFileMergeInfo.h"
#include <algorithm>
#include "TCollectionProxyInfo.h"
/*******************************************************************/

#include "TDataMember.h"

// Since CINT ignores the std namespace, we need to do so in this file.
namespace std {} using namespace std;

// Header files passed as explicit arguments
#include "MELALinearInterpFunc.h"
#include "MELANCSplineFactory_1D.h"
#include "MELANCSplineFactory_2D.h"
#include "MELANCSplineFactory_3D.h"
#include "MELAFuncPdf.h"
#include "Mela.h"

// Header files passed via #pragma extra_include

namespace TVar {
   namespace ROOT {
      inline ::ROOT::TGenericClassInfo *GenerateInitInstance();
      static TClass *TVar_Dictionary();

      // Function generating the singleton type initializer
      inline ::ROOT::TGenericClassInfo *GenerateInitInstance()
      {
         static ::ROOT::TGenericClassInfo 
            instance("TVar", 0 /*version*/, "TVar.hh", 33,
                     ::ROOT::Internal::DefineBehavior((void*)0,(void*)0),
                     &TVar_Dictionary, 0);
         return &instance;
      }
      // Insure that the inline function is _not_ optimized away by the compiler
      ::ROOT::TGenericClassInfo *(*_R__UNIQUE_DICT_(InitFunctionKeeper))() = &GenerateInitInstance;  
      // Static variable to force the class initialization
      static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstance(); R__UseDummy(_R__UNIQUE_DICT_(Init));

      // Dictionary for non-ClassDef classes
      static TClass *TVar_Dictionary() {
         return GenerateInitInstance()->GetClass();
      }

   }
}

namespace TUtil {
   namespace ROOT {
      inline ::ROOT::TGenericClassInfo *GenerateInitInstance();
      static TClass *TUtil_Dictionary();

      // Function generating the singleton type initializer
      inline ::ROOT::TGenericClassInfo *GenerateInitInstance()
      {
         static ::ROOT::TGenericClassInfo 
            instance("TUtil", 0 /*version*/, "TUtil.hh", 48,
                     ::ROOT::Internal::DefineBehavior((void*)0,(void*)0),
                     &TUtil_Dictionary, 0);
         return &instance;
      }
      // Insure that the inline function is _not_ optimized away by the compiler
      ::ROOT::TGenericClassInfo *(*_R__UNIQUE_DICT_(InitFunctionKeeper))() = &GenerateInitInstance;  
      // Static variable to force the class initialization
      static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstance(); R__UseDummy(_R__UNIQUE_DICT_(Init));

      // Dictionary for non-ClassDef classes
      static TClass *TUtil_Dictionary() {
         return GenerateInitInstance()->GetClass();
      }

   }
}

namespace ROOT {
   static void *new_MELALinearInterpFunc(void *p = 0);
   static void *newArray_MELALinearInterpFunc(Long_t size, void *p);
   static void delete_MELALinearInterpFunc(void *p);
   static void deleteArray_MELALinearInterpFunc(void *p);
   static void destruct_MELALinearInterpFunc(void *p);
   static void streamer_MELALinearInterpFunc(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELALinearInterpFunc*)
   {
      ::MELALinearInterpFunc *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELALinearInterpFunc >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELALinearInterpFunc", ::MELALinearInterpFunc::Class_Version(), "MELALinearInterpFunc.h", 13,
                  typeid(::MELALinearInterpFunc), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELALinearInterpFunc::Dictionary, isa_proxy, 16,
                  sizeof(::MELALinearInterpFunc) );
      instance.SetNew(&new_MELALinearInterpFunc);
      instance.SetNewArray(&newArray_MELALinearInterpFunc);
      instance.SetDelete(&delete_MELALinearInterpFunc);
      instance.SetDeleteArray(&deleteArray_MELALinearInterpFunc);
      instance.SetDestructor(&destruct_MELALinearInterpFunc);
      instance.SetStreamerFunc(&streamer_MELALinearInterpFunc);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELALinearInterpFunc*)
   {
      return GenerateInitInstanceLocal((::MELALinearInterpFunc*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELALinearInterpFunc*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static void delete_MELANCSplineCore(void *p);
   static void deleteArray_MELANCSplineCore(void *p);
   static void destruct_MELANCSplineCore(void *p);
   static void streamer_MELANCSplineCore(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSplineCore*)
   {
      ::MELANCSplineCore *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELANCSplineCore >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSplineCore", ::MELANCSplineCore::Class_Version(), "MELANCSplineCore.h", 15,
                  typeid(::MELANCSplineCore), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELANCSplineCore::Dictionary, isa_proxy, 16,
                  sizeof(::MELANCSplineCore) );
      instance.SetDelete(&delete_MELANCSplineCore);
      instance.SetDeleteArray(&deleteArray_MELANCSplineCore);
      instance.SetDestructor(&destruct_MELANCSplineCore);
      instance.SetStreamerFunc(&streamer_MELANCSplineCore);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSplineCore*)
   {
      return GenerateInitInstanceLocal((::MELANCSplineCore*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSplineCore*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static void *new_MELANCSpline_1D_fast(void *p = 0);
   static void *newArray_MELANCSpline_1D_fast(Long_t size, void *p);
   static void delete_MELANCSpline_1D_fast(void *p);
   static void deleteArray_MELANCSpline_1D_fast(void *p);
   static void destruct_MELANCSpline_1D_fast(void *p);
   static void streamer_MELANCSpline_1D_fast(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSpline_1D_fast*)
   {
      ::MELANCSpline_1D_fast *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELANCSpline_1D_fast >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSpline_1D_fast", ::MELANCSpline_1D_fast::Class_Version(), "MELANCSpline_1D_fast.h", 12,
                  typeid(::MELANCSpline_1D_fast), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELANCSpline_1D_fast::Dictionary, isa_proxy, 16,
                  sizeof(::MELANCSpline_1D_fast) );
      instance.SetNew(&new_MELANCSpline_1D_fast);
      instance.SetNewArray(&newArray_MELANCSpline_1D_fast);
      instance.SetDelete(&delete_MELANCSpline_1D_fast);
      instance.SetDeleteArray(&deleteArray_MELANCSpline_1D_fast);
      instance.SetDestructor(&destruct_MELANCSpline_1D_fast);
      instance.SetStreamerFunc(&streamer_MELANCSpline_1D_fast);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSpline_1D_fast*)
   {
      return GenerateInitInstanceLocal((::MELANCSpline_1D_fast*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSpline_1D_fast*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static void *new_MELAFuncPdf(void *p = 0);
   static void *newArray_MELAFuncPdf(Long_t size, void *p);
   static void delete_MELAFuncPdf(void *p);
   static void deleteArray_MELAFuncPdf(void *p);
   static void destruct_MELAFuncPdf(void *p);
   static void streamer_MELAFuncPdf(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELAFuncPdf*)
   {
      ::MELAFuncPdf *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELAFuncPdf >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELAFuncPdf", ::MELAFuncPdf::Class_Version(), "MELAFuncPdf.h", 7,
                  typeid(::MELAFuncPdf), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELAFuncPdf::Dictionary, isa_proxy, 16,
                  sizeof(::MELAFuncPdf) );
      instance.SetNew(&new_MELAFuncPdf);
      instance.SetNewArray(&newArray_MELAFuncPdf);
      instance.SetDelete(&delete_MELAFuncPdf);
      instance.SetDeleteArray(&deleteArray_MELAFuncPdf);
      instance.SetDestructor(&destruct_MELAFuncPdf);
      instance.SetStreamerFunc(&streamer_MELAFuncPdf);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELAFuncPdf*)
   {
      return GenerateInitInstanceLocal((::MELAFuncPdf*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELAFuncPdf*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static TClass *MELANCSplineFactory_1D_Dictionary();
   static void MELANCSplineFactory_1D_TClassManip(TClass*);
   static void delete_MELANCSplineFactory_1D(void *p);
   static void deleteArray_MELANCSplineFactory_1D(void *p);
   static void destruct_MELANCSplineFactory_1D(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSplineFactory_1D*)
   {
      ::MELANCSplineFactory_1D *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELANCSplineFactory_1D));
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSplineFactory_1D", "MELANCSplineFactory_1D.h", 12,
                  typeid(::MELANCSplineFactory_1D), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELANCSplineFactory_1D_Dictionary, isa_proxy, 0,
                  sizeof(::MELANCSplineFactory_1D) );
      instance.SetDelete(&delete_MELANCSplineFactory_1D);
      instance.SetDeleteArray(&deleteArray_MELANCSplineFactory_1D);
      instance.SetDestructor(&destruct_MELANCSplineFactory_1D);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSplineFactory_1D*)
   {
      return GenerateInitInstanceLocal((::MELANCSplineFactory_1D*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSplineFactory_1D*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELANCSplineFactory_1D_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineFactory_1D*)0x0)->GetClass();
      MELANCSplineFactory_1D_TClassManip(theClass);
   return theClass;
   }

   static void MELANCSplineFactory_1D_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static void *new_MELANCSpline_2D_fast(void *p = 0);
   static void *newArray_MELANCSpline_2D_fast(Long_t size, void *p);
   static void delete_MELANCSpline_2D_fast(void *p);
   static void deleteArray_MELANCSpline_2D_fast(void *p);
   static void destruct_MELANCSpline_2D_fast(void *p);
   static void streamer_MELANCSpline_2D_fast(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSpline_2D_fast*)
   {
      ::MELANCSpline_2D_fast *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELANCSpline_2D_fast >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSpline_2D_fast", ::MELANCSpline_2D_fast::Class_Version(), "MELANCSpline_2D_fast.h", 11,
                  typeid(::MELANCSpline_2D_fast), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELANCSpline_2D_fast::Dictionary, isa_proxy, 16,
                  sizeof(::MELANCSpline_2D_fast) );
      instance.SetNew(&new_MELANCSpline_2D_fast);
      instance.SetNewArray(&newArray_MELANCSpline_2D_fast);
      instance.SetDelete(&delete_MELANCSpline_2D_fast);
      instance.SetDeleteArray(&deleteArray_MELANCSpline_2D_fast);
      instance.SetDestructor(&destruct_MELANCSpline_2D_fast);
      instance.SetStreamerFunc(&streamer_MELANCSpline_2D_fast);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSpline_2D_fast*)
   {
      return GenerateInitInstanceLocal((::MELANCSpline_2D_fast*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSpline_2D_fast*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static TClass *MELANCSplineFactory_2D_Dictionary();
   static void MELANCSplineFactory_2D_TClassManip(TClass*);
   static void delete_MELANCSplineFactory_2D(void *p);
   static void deleteArray_MELANCSplineFactory_2D(void *p);
   static void destruct_MELANCSplineFactory_2D(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSplineFactory_2D*)
   {
      ::MELANCSplineFactory_2D *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELANCSplineFactory_2D));
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSplineFactory_2D", "MELANCSplineFactory_2D.h", 14,
                  typeid(::MELANCSplineFactory_2D), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELANCSplineFactory_2D_Dictionary, isa_proxy, 0,
                  sizeof(::MELANCSplineFactory_2D) );
      instance.SetDelete(&delete_MELANCSplineFactory_2D);
      instance.SetDeleteArray(&deleteArray_MELANCSplineFactory_2D);
      instance.SetDestructor(&destruct_MELANCSplineFactory_2D);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSplineFactory_2D*)
   {
      return GenerateInitInstanceLocal((::MELANCSplineFactory_2D*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSplineFactory_2D*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELANCSplineFactory_2D_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineFactory_2D*)0x0)->GetClass();
      MELANCSplineFactory_2D_TClassManip(theClass);
   return theClass;
   }

   static void MELANCSplineFactory_2D_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static void *new_MELANCSpline_3D_fast(void *p = 0);
   static void *newArray_MELANCSpline_3D_fast(Long_t size, void *p);
   static void delete_MELANCSpline_3D_fast(void *p);
   static void deleteArray_MELANCSpline_3D_fast(void *p);
   static void destruct_MELANCSpline_3D_fast(void *p);
   static void streamer_MELANCSpline_3D_fast(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSpline_3D_fast*)
   {
      ::MELANCSpline_3D_fast *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MELANCSpline_3D_fast >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSpline_3D_fast", ::MELANCSpline_3D_fast::Class_Version(), "MELANCSpline_3D_fast.h", 11,
                  typeid(::MELANCSpline_3D_fast), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MELANCSpline_3D_fast::Dictionary, isa_proxy, 16,
                  sizeof(::MELANCSpline_3D_fast) );
      instance.SetNew(&new_MELANCSpline_3D_fast);
      instance.SetNewArray(&newArray_MELANCSpline_3D_fast);
      instance.SetDelete(&delete_MELANCSpline_3D_fast);
      instance.SetDeleteArray(&deleteArray_MELANCSpline_3D_fast);
      instance.SetDestructor(&destruct_MELANCSpline_3D_fast);
      instance.SetStreamerFunc(&streamer_MELANCSpline_3D_fast);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSpline_3D_fast*)
   {
      return GenerateInitInstanceLocal((::MELANCSpline_3D_fast*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSpline_3D_fast*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static TClass *MELANCSplineFactory_3D_Dictionary();
   static void MELANCSplineFactory_3D_TClassManip(TClass*);
   static void delete_MELANCSplineFactory_3D(void *p);
   static void deleteArray_MELANCSplineFactory_3D(void *p);
   static void destruct_MELANCSplineFactory_3D(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELANCSplineFactory_3D*)
   {
      ::MELANCSplineFactory_3D *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELANCSplineFactory_3D));
      static ::ROOT::TGenericClassInfo 
         instance("MELANCSplineFactory_3D", "MELANCSplineFactory_3D.h", 14,
                  typeid(::MELANCSplineFactory_3D), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELANCSplineFactory_3D_Dictionary, isa_proxy, 0,
                  sizeof(::MELANCSplineFactory_3D) );
      instance.SetDelete(&delete_MELANCSplineFactory_3D);
      instance.SetDeleteArray(&deleteArray_MELANCSplineFactory_3D);
      instance.SetDestructor(&destruct_MELANCSplineFactory_3D);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELANCSplineFactory_3D*)
   {
      return GenerateInitInstanceLocal((::MELANCSplineFactory_3D*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELANCSplineFactory_3D*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELANCSplineFactory_3D_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineFactory_3D*)0x0)->GetClass();
      MELANCSplineFactory_3D_TClassManip(theClass);
   return theClass;
   }

   static void MELANCSplineFactory_3D_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *SpinZeroCouplings_Dictionary();
   static void SpinZeroCouplings_TClassManip(TClass*);
   static void *new_SpinZeroCouplings(void *p = 0);
   static void *newArray_SpinZeroCouplings(Long_t size, void *p);
   static void delete_SpinZeroCouplings(void *p);
   static void deleteArray_SpinZeroCouplings(void *p);
   static void destruct_SpinZeroCouplings(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::SpinZeroCouplings*)
   {
      ::SpinZeroCouplings *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::SpinZeroCouplings));
      static ::ROOT::TGenericClassInfo 
         instance("SpinZeroCouplings", "TCouplings.hh", 7,
                  typeid(::SpinZeroCouplings), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &SpinZeroCouplings_Dictionary, isa_proxy, 0,
                  sizeof(::SpinZeroCouplings) );
      instance.SetNew(&new_SpinZeroCouplings);
      instance.SetNewArray(&newArray_SpinZeroCouplings);
      instance.SetDelete(&delete_SpinZeroCouplings);
      instance.SetDeleteArray(&deleteArray_SpinZeroCouplings);
      instance.SetDestructor(&destruct_SpinZeroCouplings);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::SpinZeroCouplings*)
   {
      return GenerateInitInstanceLocal((::SpinZeroCouplings*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::SpinZeroCouplings*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *SpinZeroCouplings_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::SpinZeroCouplings*)0x0)->GetClass();
      SpinZeroCouplings_TClassManip(theClass);
   return theClass;
   }

   static void SpinZeroCouplings_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *SpinOneCouplings_Dictionary();
   static void SpinOneCouplings_TClassManip(TClass*);
   static void *new_SpinOneCouplings(void *p = 0);
   static void *newArray_SpinOneCouplings(Long_t size, void *p);
   static void delete_SpinOneCouplings(void *p);
   static void deleteArray_SpinOneCouplings(void *p);
   static void destruct_SpinOneCouplings(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::SpinOneCouplings*)
   {
      ::SpinOneCouplings *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::SpinOneCouplings));
      static ::ROOT::TGenericClassInfo 
         instance("SpinOneCouplings", "TCouplings.hh", 64,
                  typeid(::SpinOneCouplings), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &SpinOneCouplings_Dictionary, isa_proxy, 0,
                  sizeof(::SpinOneCouplings) );
      instance.SetNew(&new_SpinOneCouplings);
      instance.SetNewArray(&newArray_SpinOneCouplings);
      instance.SetDelete(&delete_SpinOneCouplings);
      instance.SetDeleteArray(&deleteArray_SpinOneCouplings);
      instance.SetDestructor(&destruct_SpinOneCouplings);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::SpinOneCouplings*)
   {
      return GenerateInitInstanceLocal((::SpinOneCouplings*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::SpinOneCouplings*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *SpinOneCouplings_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::SpinOneCouplings*)0x0)->GetClass();
      SpinOneCouplings_TClassManip(theClass);
   return theClass;
   }

   static void SpinOneCouplings_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *SpinTwoCouplings_Dictionary();
   static void SpinTwoCouplings_TClassManip(TClass*);
   static void *new_SpinTwoCouplings(void *p = 0);
   static void *newArray_SpinTwoCouplings(Long_t size, void *p);
   static void delete_SpinTwoCouplings(void *p);
   static void deleteArray_SpinTwoCouplings(void *p);
   static void destruct_SpinTwoCouplings(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::SpinTwoCouplings*)
   {
      ::SpinTwoCouplings *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::SpinTwoCouplings));
      static ::ROOT::TGenericClassInfo 
         instance("SpinTwoCouplings", "TCouplings.hh", 80,
                  typeid(::SpinTwoCouplings), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &SpinTwoCouplings_Dictionary, isa_proxy, 0,
                  sizeof(::SpinTwoCouplings) );
      instance.SetNew(&new_SpinTwoCouplings);
      instance.SetNewArray(&newArray_SpinTwoCouplings);
      instance.SetDelete(&delete_SpinTwoCouplings);
      instance.SetDeleteArray(&deleteArray_SpinTwoCouplings);
      instance.SetDestructor(&destruct_SpinTwoCouplings);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::SpinTwoCouplings*)
   {
      return GenerateInitInstanceLocal((::SpinTwoCouplings*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::SpinTwoCouplings*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *SpinTwoCouplings_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::SpinTwoCouplings*)0x0)->GetClass();
      SpinTwoCouplings_TClassManip(theClass);
   return theClass;
   }

   static void SpinTwoCouplings_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *VprimeCouplings_Dictionary();
   static void VprimeCouplings_TClassManip(TClass*);
   static void *new_VprimeCouplings(void *p = 0);
   static void *newArray_VprimeCouplings(Long_t size, void *p);
   static void delete_VprimeCouplings(void *p);
   static void deleteArray_VprimeCouplings(void *p);
   static void destruct_VprimeCouplings(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::VprimeCouplings*)
   {
      ::VprimeCouplings *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::VprimeCouplings));
      static ::ROOT::TGenericClassInfo 
         instance("VprimeCouplings", "TCouplings.hh", 102,
                  typeid(::VprimeCouplings), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &VprimeCouplings_Dictionary, isa_proxy, 0,
                  sizeof(::VprimeCouplings) );
      instance.SetNew(&new_VprimeCouplings);
      instance.SetNewArray(&newArray_VprimeCouplings);
      instance.SetDelete(&delete_VprimeCouplings);
      instance.SetDeleteArray(&deleteArray_VprimeCouplings);
      instance.SetDestructor(&destruct_VprimeCouplings);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::VprimeCouplings*)
   {
      return GenerateInitInstanceLocal((::VprimeCouplings*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::VprimeCouplings*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *VprimeCouplings_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::VprimeCouplings*)0x0)->GetClass();
      VprimeCouplings_TClassManip(theClass);
   return theClass;
   }

   static void VprimeCouplings_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *MELAParticle_Dictionary();
   static void MELAParticle_TClassManip(TClass*);
   static void *new_MELAParticle(void *p = 0);
   static void *newArray_MELAParticle(Long_t size, void *p);
   static void delete_MELAParticle(void *p);
   static void deleteArray_MELAParticle(void *p);
   static void destruct_MELAParticle(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELAParticle*)
   {
      ::MELAParticle *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELAParticle));
      static ::ROOT::TGenericClassInfo 
         instance("MELAParticle", "MELAParticle.h", 13,
                  typeid(::MELAParticle), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELAParticle_Dictionary, isa_proxy, 0,
                  sizeof(::MELAParticle) );
      instance.SetNew(&new_MELAParticle);
      instance.SetNewArray(&newArray_MELAParticle);
      instance.SetDelete(&delete_MELAParticle);
      instance.SetDeleteArray(&deleteArray_MELAParticle);
      instance.SetDestructor(&destruct_MELAParticle);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELAParticle*)
   {
      return GenerateInitInstanceLocal((::MELAParticle*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELAParticle*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELAParticle_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELAParticle*)0x0)->GetClass();
      MELAParticle_TClassManip(theClass);
   return theClass;
   }

   static void MELAParticle_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *MELATopCandidate_Dictionary();
   static void MELATopCandidate_TClassManip(TClass*);
   static void *new_MELATopCandidate(void *p = 0);
   static void *newArray_MELATopCandidate(Long_t size, void *p);
   static void delete_MELATopCandidate(void *p);
   static void deleteArray_MELATopCandidate(void *p);
   static void destruct_MELATopCandidate(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELATopCandidate*)
   {
      ::MELATopCandidate *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELATopCandidate));
      static ::ROOT::TGenericClassInfo 
         instance("MELATopCandidate", "MELATopCandidate.h", 7,
                  typeid(::MELATopCandidate), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELATopCandidate_Dictionary, isa_proxy, 0,
                  sizeof(::MELATopCandidate) );
      instance.SetNew(&new_MELATopCandidate);
      instance.SetNewArray(&newArray_MELATopCandidate);
      instance.SetDelete(&delete_MELATopCandidate);
      instance.SetDeleteArray(&deleteArray_MELATopCandidate);
      instance.SetDestructor(&destruct_MELATopCandidate);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELATopCandidate*)
   {
      return GenerateInitInstanceLocal((::MELATopCandidate*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELATopCandidate*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELATopCandidate_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELATopCandidate*)0x0)->GetClass();
      MELATopCandidate_TClassManip(theClass);
   return theClass;
   }

   static void MELATopCandidate_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static TClass *MELACandidate_Dictionary();
   static void MELACandidate_TClassManip(TClass*);
   static void *new_MELACandidate(void *p = 0);
   static void *newArray_MELACandidate(Long_t size, void *p);
   static void delete_MELACandidate(void *p);
   static void deleteArray_MELACandidate(void *p);
   static void destruct_MELACandidate(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MELACandidate*)
   {
      ::MELACandidate *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::MELACandidate));
      static ::ROOT::TGenericClassInfo 
         instance("MELACandidate", "MELACandidate.h", 7,
                  typeid(::MELACandidate), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &MELACandidate_Dictionary, isa_proxy, 0,
                  sizeof(::MELACandidate) );
      instance.SetNew(&new_MELACandidate);
      instance.SetNewArray(&newArray_MELACandidate);
      instance.SetDelete(&delete_MELACandidate);
      instance.SetDeleteArray(&deleteArray_MELACandidate);
      instance.SetDestructor(&destruct_MELACandidate);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MELACandidate*)
   {
      return GenerateInitInstanceLocal((::MELACandidate*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MELACandidate*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *MELACandidate_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::MELACandidate*)0x0)->GetClass();
      MELACandidate_TClassManip(theClass);
   return theClass;
   }

   static void MELACandidate_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   static void *new_MelaIO(void *p = 0);
   static void *newArray_MelaIO(Long_t size, void *p);
   static void delete_MelaIO(void *p);
   static void deleteArray_MelaIO(void *p);
   static void destruct_MelaIO(void *p);
   static void streamer_MelaIO(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::MelaIO*)
   {
      ::MelaIO *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::MelaIO >(0);
      static ::ROOT::TGenericClassInfo 
         instance("MelaIO", ::MelaIO::Class_Version(), "MelaIO.h", 8,
                  typeid(::MelaIO), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::MelaIO::Dictionary, isa_proxy, 16,
                  sizeof(::MelaIO) );
      instance.SetNew(&new_MelaIO);
      instance.SetNewArray(&newArray_MelaIO);
      instance.SetDelete(&delete_MelaIO);
      instance.SetDeleteArray(&deleteArray_MelaIO);
      instance.SetDestructor(&destruct_MelaIO);
      instance.SetStreamerFunc(&streamer_MelaIO);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::MelaIO*)
   {
      return GenerateInitInstanceLocal((::MelaIO*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::MelaIO*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static void *new_TEvtProb(void *p = 0);
   static void *newArray_TEvtProb(Long_t size, void *p);
   static void delete_TEvtProb(void *p);
   static void deleteArray_TEvtProb(void *p);
   static void destruct_TEvtProb(void *p);
   static void streamer_TEvtProb(TBuffer &buf, void *obj);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::TEvtProb*)
   {
      ::TEvtProb *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TInstrumentedIsAProxy< ::TEvtProb >(0);
      static ::ROOT::TGenericClassInfo 
         instance("TEvtProb", ::TEvtProb::Class_Version(), "TEvtProb.hh", 48,
                  typeid(::TEvtProb), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &::TEvtProb::Dictionary, isa_proxy, 16,
                  sizeof(::TEvtProb) );
      instance.SetNew(&new_TEvtProb);
      instance.SetNewArray(&newArray_TEvtProb);
      instance.SetDelete(&delete_TEvtProb);
      instance.SetDeleteArray(&deleteArray_TEvtProb);
      instance.SetDestructor(&destruct_TEvtProb);
      instance.SetStreamerFunc(&streamer_TEvtProb);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::TEvtProb*)
   {
      return GenerateInitInstanceLocal((::TEvtProb*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::TEvtProb*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));
} // end of namespace ROOT

namespace ROOT {
   static TClass *Mela_Dictionary();
   static void Mela_TClassManip(TClass*);
   static void *new_Mela(void *p = 0);
   static void *newArray_Mela(Long_t size, void *p);
   static void delete_Mela(void *p);
   static void deleteArray_Mela(void *p);
   static void destruct_Mela(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const ::Mela*)
   {
      ::Mela *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(::Mela));
      static ::ROOT::TGenericClassInfo 
         instance("Mela", "Mela.h", 38,
                  typeid(::Mela), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &Mela_Dictionary, isa_proxy, 0,
                  sizeof(::Mela) );
      instance.SetNew(&new_Mela);
      instance.SetNewArray(&newArray_Mela);
      instance.SetDelete(&delete_Mela);
      instance.SetDeleteArray(&deleteArray_Mela);
      instance.SetDestructor(&destruct_Mela);
      return &instance;
   }
   TGenericClassInfo *GenerateInitInstance(const ::Mela*)
   {
      return GenerateInitInstanceLocal((::Mela*)0);
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const ::Mela*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *Mela_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const ::Mela*)0x0)->GetClass();
      Mela_TClassManip(theClass);
   return theClass;
   }

   static void Mela_TClassManip(TClass* ){
   }

} // end of namespace ROOT

//______________________________________________________________________________
atomic_TClass_ptr MELALinearInterpFunc::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELALinearInterpFunc::Class_Name()
{
   return "MELALinearInterpFunc";
}

//______________________________________________________________________________
const char *MELALinearInterpFunc::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELALinearInterpFunc*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELALinearInterpFunc::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELALinearInterpFunc*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELALinearInterpFunc::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELALinearInterpFunc*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELALinearInterpFunc::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELALinearInterpFunc*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MELANCSplineCore::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELANCSplineCore::Class_Name()
{
   return "MELANCSplineCore";
}

//______________________________________________________________________________
const char *MELANCSplineCore::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineCore*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELANCSplineCore::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineCore*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELANCSplineCore::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineCore*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELANCSplineCore::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSplineCore*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MELANCSpline_1D_fast::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELANCSpline_1D_fast::Class_Name()
{
   return "MELANCSpline_1D_fast";
}

//______________________________________________________________________________
const char *MELANCSpline_1D_fast::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_1D_fast*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELANCSpline_1D_fast::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_1D_fast*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELANCSpline_1D_fast::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_1D_fast*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELANCSpline_1D_fast::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_1D_fast*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MELAFuncPdf::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELAFuncPdf::Class_Name()
{
   return "MELAFuncPdf";
}

//______________________________________________________________________________
const char *MELAFuncPdf::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELAFuncPdf*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELAFuncPdf::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELAFuncPdf*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELAFuncPdf::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELAFuncPdf*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELAFuncPdf::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELAFuncPdf*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MELANCSpline_2D_fast::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELANCSpline_2D_fast::Class_Name()
{
   return "MELANCSpline_2D_fast";
}

//______________________________________________________________________________
const char *MELANCSpline_2D_fast::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_2D_fast*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELANCSpline_2D_fast::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_2D_fast*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELANCSpline_2D_fast::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_2D_fast*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELANCSpline_2D_fast::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_2D_fast*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MELANCSpline_3D_fast::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MELANCSpline_3D_fast::Class_Name()
{
   return "MELANCSpline_3D_fast";
}

//______________________________________________________________________________
const char *MELANCSpline_3D_fast::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_3D_fast*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MELANCSpline_3D_fast::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_3D_fast*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MELANCSpline_3D_fast::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_3D_fast*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MELANCSpline_3D_fast::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MELANCSpline_3D_fast*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr MelaIO::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *MelaIO::Class_Name()
{
   return "MelaIO";
}

//______________________________________________________________________________
const char *MelaIO::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MelaIO*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int MelaIO::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::MelaIO*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *MelaIO::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MelaIO*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *MelaIO::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::MelaIO*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
atomic_TClass_ptr TEvtProb::fgIsA(0);  // static to hold class pointer

//______________________________________________________________________________
const char *TEvtProb::Class_Name()
{
   return "TEvtProb";
}

//______________________________________________________________________________
const char *TEvtProb::ImplFileName()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::TEvtProb*)0x0)->GetImplFileName();
}

//______________________________________________________________________________
int TEvtProb::ImplFileLine()
{
   return ::ROOT::GenerateInitInstanceLocal((const ::TEvtProb*)0x0)->GetImplFileLine();
}

//______________________________________________________________________________
TClass *TEvtProb::Dictionary()
{
   fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::TEvtProb*)0x0)->GetClass();
   return fgIsA;
}

//______________________________________________________________________________
TClass *TEvtProb::Class()
{
   if (!fgIsA.load()) { R__LOCKGUARD2(gInterpreterMutex); fgIsA = ::ROOT::GenerateInitInstanceLocal((const ::TEvtProb*)0x0)->GetClass(); }
   return fgIsA;
}

//______________________________________________________________________________
void MELALinearInterpFunc::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELALinearInterpFunc.

   RooAbsReal::Streamer(R__b);
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELALinearInterpFunc(void *p) {
      return  p ? new(p) ::MELALinearInterpFunc : new ::MELALinearInterpFunc;
   }
   static void *newArray_MELALinearInterpFunc(Long_t nElements, void *p) {
      return p ? new(p) ::MELALinearInterpFunc[nElements] : new ::MELALinearInterpFunc[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELALinearInterpFunc(void *p) {
      delete ((::MELALinearInterpFunc*)p);
   }
   static void deleteArray_MELALinearInterpFunc(void *p) {
      delete [] ((::MELALinearInterpFunc*)p);
   }
   static void destruct_MELALinearInterpFunc(void *p) {
      typedef ::MELALinearInterpFunc current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELALinearInterpFunc(TBuffer &buf, void *obj) {
      ((::MELALinearInterpFunc*)obj)->::MELALinearInterpFunc::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELALinearInterpFunc

//______________________________________________________________________________
void MELANCSplineCore::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELANCSplineCore.

   RooAbsReal::Streamer(R__b);
}

namespace ROOT {
   // Wrapper around operator delete
   static void delete_MELANCSplineCore(void *p) {
      delete ((::MELANCSplineCore*)p);
   }
   static void deleteArray_MELANCSplineCore(void *p) {
      delete [] ((::MELANCSplineCore*)p);
   }
   static void destruct_MELANCSplineCore(void *p) {
      typedef ::MELANCSplineCore current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELANCSplineCore(TBuffer &buf, void *obj) {
      ((::MELANCSplineCore*)obj)->::MELANCSplineCore::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELANCSplineCore

//______________________________________________________________________________
void MELANCSpline_1D_fast::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELANCSpline_1D_fast.

   UInt_t R__s, R__c;
   if (R__b.IsReading()) {
      Version_t R__v = R__b.ReadVersion(&R__s, &R__c); if (R__v) { }
      MELANCSplineCore::Streamer(R__b);
      void *ptr_bcBeginX = (void*)&bcBeginX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginX);
      void *ptr_bcEndX = (void*)&bcEndX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndX);
      {
         vector<MELANCSplineCore::T> &R__stl =  FcnList;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<vector<MELANCSplineCore::T> > &R__stl =  coefficients;
         R__stl.clear();
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<float, class std::allocator<float> >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<float, class std::allocator<float> >!");
            return;
         }
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            vector<float> R__t;
            R__b.StreamObject(&R__t,R__tcl1);
            R__stl.push_back(R__t);
         }
      }
      R__b.CheckByteCount(R__s, R__c, MELANCSpline_1D_fast::IsA());
   } else {
      R__c = R__b.WriteVersion(MELANCSpline_1D_fast::IsA(), kTRUE);
      MELANCSplineCore::Streamer(R__b);
      R__b << (Int_t)bcBeginX;
      R__b << (Int_t)bcEndX;
      {
         vector<MELANCSplineCore::T> &R__stl =  FcnList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<vector<MELANCSplineCore::T> > &R__stl =  coefficients;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<float, class std::allocator<float> >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<float, class std::allocator<float> >!");
            return;
         }
            vector<vector<MELANCSplineCore::T> >::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b.StreamObject((vector<float>*)&(*R__k),R__tcl1);
            }
         }
      }
      R__b.SetByteCount(R__c, kTRUE);
   }
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELANCSpline_1D_fast(void *p) {
      return  p ? new(p) ::MELANCSpline_1D_fast : new ::MELANCSpline_1D_fast;
   }
   static void *newArray_MELANCSpline_1D_fast(Long_t nElements, void *p) {
      return p ? new(p) ::MELANCSpline_1D_fast[nElements] : new ::MELANCSpline_1D_fast[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELANCSpline_1D_fast(void *p) {
      delete ((::MELANCSpline_1D_fast*)p);
   }
   static void deleteArray_MELANCSpline_1D_fast(void *p) {
      delete [] ((::MELANCSpline_1D_fast*)p);
   }
   static void destruct_MELANCSpline_1D_fast(void *p) {
      typedef ::MELANCSpline_1D_fast current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELANCSpline_1D_fast(TBuffer &buf, void *obj) {
      ((::MELANCSpline_1D_fast*)obj)->::MELANCSpline_1D_fast::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELANCSpline_1D_fast

//______________________________________________________________________________
void MELAFuncPdf::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELAFuncPdf.

   RooAbsPdf::Streamer(R__b);
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELAFuncPdf(void *p) {
      return  p ? new(p) ::MELAFuncPdf : new ::MELAFuncPdf;
   }
   static void *newArray_MELAFuncPdf(Long_t nElements, void *p) {
      return p ? new(p) ::MELAFuncPdf[nElements] : new ::MELAFuncPdf[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELAFuncPdf(void *p) {
      delete ((::MELAFuncPdf*)p);
   }
   static void deleteArray_MELAFuncPdf(void *p) {
      delete [] ((::MELAFuncPdf*)p);
   }
   static void destruct_MELAFuncPdf(void *p) {
      typedef ::MELAFuncPdf current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELAFuncPdf(TBuffer &buf, void *obj) {
      ((::MELAFuncPdf*)obj)->::MELAFuncPdf::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELAFuncPdf

namespace ROOT {
   // Wrapper around operator delete
   static void delete_MELANCSplineFactory_1D(void *p) {
      delete ((::MELANCSplineFactory_1D*)p);
   }
   static void deleteArray_MELANCSplineFactory_1D(void *p) {
      delete [] ((::MELANCSplineFactory_1D*)p);
   }
   static void destruct_MELANCSplineFactory_1D(void *p) {
      typedef ::MELANCSplineFactory_1D current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELANCSplineFactory_1D

//______________________________________________________________________________
void MELANCSpline_2D_fast::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELANCSpline_2D_fast.

   UInt_t R__s, R__c;
   if (R__b.IsReading()) {
      Version_t R__v = R__b.ReadVersion(&R__s, &R__c); if (R__v) { }
      MELANCSplineCore::Streamer(R__b);
      R__b >> rangeYmin;
      R__b >> rangeYmax;
      void *ptr_bcBeginX = (void*)&bcBeginX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginX);
      void *ptr_bcEndX = (void*)&bcEndX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndX);
      void *ptr_bcBeginY = (void*)&bcBeginY;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginY);
      void *ptr_bcEndY = (void*)&bcEndY;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndY);
      theYVar.Streamer(R__b);
      {
         vector<MELANCSplineCore::T> &R__stl =  YList;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<vector<MELANCSplineCore::T> > &R__stl =  FcnList;
         R__stl.clear();
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<float, class std::allocator<float> >));
         if (R__tcl1==0) {
            Error("FcnList streamer","Missing the TClass object for class std::vector<float, class std::allocator<float> >!");
            return;
         }
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            vector<float> R__t;
            R__b.StreamObject(&R__t,R__tcl1);
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaY;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<vector<vector<vector<MELANCSplineCore::T> > > > &R__stl =  coefficients;
         R__stl.clear();
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >!");
            return;
         }
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            vector<vector<vector<float> > > R__t;
            R__b.StreamObject(&R__t,R__tcl1);
            R__stl.push_back(R__t);
         }
      }
      R__b.CheckByteCount(R__s, R__c, MELANCSpline_2D_fast::IsA());
   } else {
      R__c = R__b.WriteVersion(MELANCSpline_2D_fast::IsA(), kTRUE);
      MELANCSplineCore::Streamer(R__b);
      R__b << rangeYmin;
      R__b << rangeYmax;
      R__b << (Int_t)bcBeginX;
      R__b << (Int_t)bcEndX;
      R__b << (Int_t)bcBeginY;
      R__b << (Int_t)bcEndY;
      theYVar.Streamer(R__b);
      {
         vector<MELANCSplineCore::T> &R__stl =  YList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<vector<MELANCSplineCore::T> > &R__stl =  FcnList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<float, class std::allocator<float> >));
         if (R__tcl1==0) {
            Error("FcnList streamer","Missing the TClass object for class std::vector<float, class std::allocator<float> >!");
            return;
         }
            vector<vector<MELANCSplineCore::T> >::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b.StreamObject((vector<float>*)&(*R__k),R__tcl1);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaY;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<vector<vector<vector<MELANCSplineCore::T> > > > &R__stl =  coefficients;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >!");
            return;
         }
            vector<vector<vector<vector<MELANCSplineCore::T> > > >::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b.StreamObject((vector<vector<vector<float> > >*)&(*R__k),R__tcl1);
            }
         }
      }
      R__b.SetByteCount(R__c, kTRUE);
   }
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELANCSpline_2D_fast(void *p) {
      return  p ? new(p) ::MELANCSpline_2D_fast : new ::MELANCSpline_2D_fast;
   }
   static void *newArray_MELANCSpline_2D_fast(Long_t nElements, void *p) {
      return p ? new(p) ::MELANCSpline_2D_fast[nElements] : new ::MELANCSpline_2D_fast[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELANCSpline_2D_fast(void *p) {
      delete ((::MELANCSpline_2D_fast*)p);
   }
   static void deleteArray_MELANCSpline_2D_fast(void *p) {
      delete [] ((::MELANCSpline_2D_fast*)p);
   }
   static void destruct_MELANCSpline_2D_fast(void *p) {
      typedef ::MELANCSpline_2D_fast current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELANCSpline_2D_fast(TBuffer &buf, void *obj) {
      ((::MELANCSpline_2D_fast*)obj)->::MELANCSpline_2D_fast::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELANCSpline_2D_fast

namespace ROOT {
   // Wrapper around operator delete
   static void delete_MELANCSplineFactory_2D(void *p) {
      delete ((::MELANCSplineFactory_2D*)p);
   }
   static void deleteArray_MELANCSplineFactory_2D(void *p) {
      delete [] ((::MELANCSplineFactory_2D*)p);
   }
   static void destruct_MELANCSplineFactory_2D(void *p) {
      typedef ::MELANCSplineFactory_2D current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELANCSplineFactory_2D

//______________________________________________________________________________
void MELANCSpline_3D_fast::Streamer(TBuffer &R__b)
{
   // Stream an object of class MELANCSpline_3D_fast.

   UInt_t R__s, R__c;
   if (R__b.IsReading()) {
      Version_t R__v = R__b.ReadVersion(&R__s, &R__c); if (R__v) { }
      MELANCSplineCore::Streamer(R__b);
      R__b >> rangeYmin;
      R__b >> rangeYmax;
      R__b >> rangeZmin;
      R__b >> rangeZmax;
      void *ptr_bcBeginX = (void*)&bcBeginX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginX);
      void *ptr_bcEndX = (void*)&bcEndX;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndX);
      void *ptr_bcBeginY = (void*)&bcBeginY;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginY);
      void *ptr_bcEndY = (void*)&bcEndY;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndY);
      void *ptr_bcBeginZ = (void*)&bcBeginZ;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcBeginZ);
      void *ptr_bcEndZ = (void*)&bcEndZ;
      R__b >> *reinterpret_cast<Int_t*>(ptr_bcEndZ);
      theYVar.Streamer(R__b);
      theZVar.Streamer(R__b);
      {
         vector<MELANCSplineCore::T> &R__stl =  YList;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  ZList;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<vector<vector<MELANCSplineCore::T> > > &R__stl =  FcnList;
         R__stl.clear();
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >));
         if (R__tcl1==0) {
            Error("FcnList streamer","Missing the TClass object for class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >!");
            return;
         }
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            vector<vector<float> > R__t;
            R__b.StreamObject(&R__t,R__tcl1);
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaY;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaZ;
         R__stl.clear();
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            float R__t;
            R__b >> R__t;
            R__stl.push_back(R__t);
         }
      }
      {
         vector<vector<vector<vector<vector<vector<MELANCSplineCore::T> > > > > > &R__stl =  coefficients;
         R__stl.clear();
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > > > >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > > > >!");
            return;
         }
         int R__i, R__n;
         R__b >> R__n;
         R__stl.reserve(R__n);
         for (R__i = 0; R__i < R__n; R__i++) {
            vector<vector<vector<vector<vector<float> > > > > R__t;
            R__b.StreamObject(&R__t,R__tcl1);
            R__stl.push_back(R__t);
         }
      }
      R__b.CheckByteCount(R__s, R__c, MELANCSpline_3D_fast::IsA());
   } else {
      R__c = R__b.WriteVersion(MELANCSpline_3D_fast::IsA(), kTRUE);
      MELANCSplineCore::Streamer(R__b);
      R__b << rangeYmin;
      R__b << rangeYmax;
      R__b << rangeZmin;
      R__b << rangeZmax;
      R__b << (Int_t)bcBeginX;
      R__b << (Int_t)bcEndX;
      R__b << (Int_t)bcBeginY;
      R__b << (Int_t)bcEndY;
      R__b << (Int_t)bcBeginZ;
      R__b << (Int_t)bcEndZ;
      theYVar.Streamer(R__b);
      theZVar.Streamer(R__b);
      {
         vector<MELANCSplineCore::T> &R__stl =  YList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  ZList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<vector<vector<MELANCSplineCore::T> > > &R__stl =  FcnList;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >));
         if (R__tcl1==0) {
            Error("FcnList streamer","Missing the TClass object for class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >!");
            return;
         }
            vector<vector<vector<MELANCSplineCore::T> > >::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b.StreamObject((vector<vector<float> >*)&(*R__k),R__tcl1);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaX;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaY;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<MELANCSplineCore::T> &R__stl =  kappaZ;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
            vector<MELANCSplineCore::T>::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b << (*R__k);
            }
         }
      }
      {
         vector<vector<vector<vector<vector<vector<MELANCSplineCore::T> > > > > > &R__stl =  coefficients;
         int R__n=int(R__stl.size());
         R__b << R__n;
         if(R__n) {
         TClass *R__tcl1 = TBuffer::GetClass(typeid(class std::vector<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > > > >));
         if (R__tcl1==0) {
            Error("coefficients streamer","Missing the TClass object for class std::vector<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > >, class std::allocator<class std::vector<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > >, class std::allocator<class std::vector<class std::vector<float, class std::allocator<float> >, class std::allocator<class std::vector<float, class std::allocator<float> > > > > > > > > >!");
            return;
         }
            vector<vector<vector<vector<vector<vector<MELANCSplineCore::T> > > > > >::iterator R__k;
            for (R__k = R__stl.begin(); R__k != R__stl.end(); ++R__k) {
            R__b.StreamObject((vector<vector<vector<vector<vector<float> > > > >*)&(*R__k),R__tcl1);
            }
         }
      }
      R__b.SetByteCount(R__c, kTRUE);
   }
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELANCSpline_3D_fast(void *p) {
      return  p ? new(p) ::MELANCSpline_3D_fast : new ::MELANCSpline_3D_fast;
   }
   static void *newArray_MELANCSpline_3D_fast(Long_t nElements, void *p) {
      return p ? new(p) ::MELANCSpline_3D_fast[nElements] : new ::MELANCSpline_3D_fast[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELANCSpline_3D_fast(void *p) {
      delete ((::MELANCSpline_3D_fast*)p);
   }
   static void deleteArray_MELANCSpline_3D_fast(void *p) {
      delete [] ((::MELANCSpline_3D_fast*)p);
   }
   static void destruct_MELANCSpline_3D_fast(void *p) {
      typedef ::MELANCSpline_3D_fast current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MELANCSpline_3D_fast(TBuffer &buf, void *obj) {
      ((::MELANCSpline_3D_fast*)obj)->::MELANCSpline_3D_fast::Streamer(buf);
   }
} // end of namespace ROOT for class ::MELANCSpline_3D_fast

namespace ROOT {
   // Wrapper around operator delete
   static void delete_MELANCSplineFactory_3D(void *p) {
      delete ((::MELANCSplineFactory_3D*)p);
   }
   static void deleteArray_MELANCSplineFactory_3D(void *p) {
      delete [] ((::MELANCSplineFactory_3D*)p);
   }
   static void destruct_MELANCSplineFactory_3D(void *p) {
      typedef ::MELANCSplineFactory_3D current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELANCSplineFactory_3D

namespace ROOT {
   // Wrappers around operator new
   static void *new_SpinZeroCouplings(void *p) {
      return  p ? new(p) ::SpinZeroCouplings : new ::SpinZeroCouplings;
   }
   static void *newArray_SpinZeroCouplings(Long_t nElements, void *p) {
      return p ? new(p) ::SpinZeroCouplings[nElements] : new ::SpinZeroCouplings[nElements];
   }
   // Wrapper around operator delete
   static void delete_SpinZeroCouplings(void *p) {
      delete ((::SpinZeroCouplings*)p);
   }
   static void deleteArray_SpinZeroCouplings(void *p) {
      delete [] ((::SpinZeroCouplings*)p);
   }
   static void destruct_SpinZeroCouplings(void *p) {
      typedef ::SpinZeroCouplings current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::SpinZeroCouplings

namespace ROOT {
   // Wrappers around operator new
   static void *new_SpinOneCouplings(void *p) {
      return  p ? new(p) ::SpinOneCouplings : new ::SpinOneCouplings;
   }
   static void *newArray_SpinOneCouplings(Long_t nElements, void *p) {
      return p ? new(p) ::SpinOneCouplings[nElements] : new ::SpinOneCouplings[nElements];
   }
   // Wrapper around operator delete
   static void delete_SpinOneCouplings(void *p) {
      delete ((::SpinOneCouplings*)p);
   }
   static void deleteArray_SpinOneCouplings(void *p) {
      delete [] ((::SpinOneCouplings*)p);
   }
   static void destruct_SpinOneCouplings(void *p) {
      typedef ::SpinOneCouplings current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::SpinOneCouplings

namespace ROOT {
   // Wrappers around operator new
   static void *new_SpinTwoCouplings(void *p) {
      return  p ? new(p) ::SpinTwoCouplings : new ::SpinTwoCouplings;
   }
   static void *newArray_SpinTwoCouplings(Long_t nElements, void *p) {
      return p ? new(p) ::SpinTwoCouplings[nElements] : new ::SpinTwoCouplings[nElements];
   }
   // Wrapper around operator delete
   static void delete_SpinTwoCouplings(void *p) {
      delete ((::SpinTwoCouplings*)p);
   }
   static void deleteArray_SpinTwoCouplings(void *p) {
      delete [] ((::SpinTwoCouplings*)p);
   }
   static void destruct_SpinTwoCouplings(void *p) {
      typedef ::SpinTwoCouplings current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::SpinTwoCouplings

namespace ROOT {
   // Wrappers around operator new
   static void *new_VprimeCouplings(void *p) {
      return  p ? new(p) ::VprimeCouplings : new ::VprimeCouplings;
   }
   static void *newArray_VprimeCouplings(Long_t nElements, void *p) {
      return p ? new(p) ::VprimeCouplings[nElements] : new ::VprimeCouplings[nElements];
   }
   // Wrapper around operator delete
   static void delete_VprimeCouplings(void *p) {
      delete ((::VprimeCouplings*)p);
   }
   static void deleteArray_VprimeCouplings(void *p) {
      delete [] ((::VprimeCouplings*)p);
   }
   static void destruct_VprimeCouplings(void *p) {
      typedef ::VprimeCouplings current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::VprimeCouplings

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELAParticle(void *p) {
      return  p ? new(p) ::MELAParticle : new ::MELAParticle;
   }
   static void *newArray_MELAParticle(Long_t nElements, void *p) {
      return p ? new(p) ::MELAParticle[nElements] : new ::MELAParticle[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELAParticle(void *p) {
      delete ((::MELAParticle*)p);
   }
   static void deleteArray_MELAParticle(void *p) {
      delete [] ((::MELAParticle*)p);
   }
   static void destruct_MELAParticle(void *p) {
      typedef ::MELAParticle current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELAParticle

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELATopCandidate(void *p) {
      return  p ? new(p) ::MELATopCandidate : new ::MELATopCandidate;
   }
   static void *newArray_MELATopCandidate(Long_t nElements, void *p) {
      return p ? new(p) ::MELATopCandidate[nElements] : new ::MELATopCandidate[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELATopCandidate(void *p) {
      delete ((::MELATopCandidate*)p);
   }
   static void deleteArray_MELATopCandidate(void *p) {
      delete [] ((::MELATopCandidate*)p);
   }
   static void destruct_MELATopCandidate(void *p) {
      typedef ::MELATopCandidate current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELATopCandidate

namespace ROOT {
   // Wrappers around operator new
   static void *new_MELACandidate(void *p) {
      return  p ? new(p) ::MELACandidate : new ::MELACandidate;
   }
   static void *newArray_MELACandidate(Long_t nElements, void *p) {
      return p ? new(p) ::MELACandidate[nElements] : new ::MELACandidate[nElements];
   }
   // Wrapper around operator delete
   static void delete_MELACandidate(void *p) {
      delete ((::MELACandidate*)p);
   }
   static void deleteArray_MELACandidate(void *p) {
      delete [] ((::MELACandidate*)p);
   }
   static void destruct_MELACandidate(void *p) {
      typedef ::MELACandidate current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::MELACandidate

//______________________________________________________________________________
void MelaIO::Streamer(TBuffer &R__b)
{
   // Stream an object of class MelaIO.

   ::Error("MelaIO::Streamer", "version id <=0 in ClassDef, dummy Streamer() called"); if (R__b.IsReading()) { }
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_MelaIO(void *p) {
      return  p ? new(p) ::MelaIO : new ::MelaIO;
   }
   static void *newArray_MelaIO(Long_t nElements, void *p) {
      return p ? new(p) ::MelaIO[nElements] : new ::MelaIO[nElements];
   }
   // Wrapper around operator delete
   static void delete_MelaIO(void *p) {
      delete ((::MelaIO*)p);
   }
   static void deleteArray_MelaIO(void *p) {
      delete [] ((::MelaIO*)p);
   }
   static void destruct_MelaIO(void *p) {
      typedef ::MelaIO current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_MelaIO(TBuffer &buf, void *obj) {
      ((::MelaIO*)obj)->::MelaIO::Streamer(buf);
   }
} // end of namespace ROOT for class ::MelaIO

//______________________________________________________________________________
void TEvtProb::Streamer(TBuffer &R__b)
{
   // Stream an object of class TEvtProb.

   TObject::Streamer(R__b);
}

namespace ROOT {
   // Wrappers around operator new
   static void *new_TEvtProb(void *p) {
      return  p ? new(p) ::TEvtProb : new ::TEvtProb;
   }
   static void *newArray_TEvtProb(Long_t nElements, void *p) {
      return p ? new(p) ::TEvtProb[nElements] : new ::TEvtProb[nElements];
   }
   // Wrapper around operator delete
   static void delete_TEvtProb(void *p) {
      delete ((::TEvtProb*)p);
   }
   static void deleteArray_TEvtProb(void *p) {
      delete [] ((::TEvtProb*)p);
   }
   static void destruct_TEvtProb(void *p) {
      typedef ::TEvtProb current_t;
      ((current_t*)p)->~current_t();
   }
   // Wrapper around a custom streamer member function.
   static void streamer_TEvtProb(TBuffer &buf, void *obj) {
      ((::TEvtProb*)obj)->::TEvtProb::Streamer(buf);
   }
} // end of namespace ROOT for class ::TEvtProb

namespace ROOT {
   // Wrappers around operator new
   static void *new_Mela(void *p) {
      return  p ? new(p) ::Mela : new ::Mela;
   }
   static void *newArray_Mela(Long_t nElements, void *p) {
      return p ? new(p) ::Mela[nElements] : new ::Mela[nElements];
   }
   // Wrapper around operator delete
   static void delete_Mela(void *p) {
      delete ((::Mela*)p);
   }
   static void deleteArray_Mela(void *p) {
      delete [] ((::Mela*)p);
   }
   static void destruct_Mela(void *p) {
      typedef ::Mela current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class ::Mela

namespace ROOT {
   static TClass *vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_Dictionary();
   static void vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_TClassManip(TClass*);
   static void *new_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p = 0);
   static void *newArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(Long_t size, void *p);
   static void delete_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p);
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p);
   static void destruct_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<vector<vector<vector<vector<vector<float> > > > > >*)
   {
      vector<vector<vector<vector<vector<vector<float> > > > > > *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<vector<vector<vector<vector<vector<float> > > > > >));
      static ::ROOT::TGenericClassInfo 
         instance("vector<vector<vector<vector<vector<vector<float> > > > > >", -2, "vector", 214,
                  typeid(vector<vector<vector<vector<vector<vector<float> > > > > >), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<vector<vector<vector<vector<vector<float> > > > > >) );
      instance.SetNew(&new_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR);
      instance.SetNewArray(&newArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR);
      instance.SetDelete(&delete_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR);
      instance.SetDeleteArray(&deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR);
      instance.SetDestructor(&destruct_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<vector<vector<vector<vector<vector<float> > > > > > >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<vector<vector<vector<vector<vector<float> > > > > >*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<vector<vector<vector<vector<vector<float> > > > > >*)0x0)->GetClass();
      vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<vector<vector<float> > > > > > : new vector<vector<vector<vector<vector<vector<float> > > > > >;
   }
   static void *newArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<vector<vector<float> > > > > >[nElements] : new vector<vector<vector<vector<vector<vector<float> > > > > >[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p) {
      delete ((vector<vector<vector<vector<vector<vector<float> > > > > >*)p);
   }
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p) {
      delete [] ((vector<vector<vector<vector<vector<vector<float> > > > > >*)p);
   }
   static void destruct_vectorlEvectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgRsPgR(void *p) {
      typedef vector<vector<vector<vector<vector<vector<float> > > > > > current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<vector<vector<vector<vector<vector<float> > > > > >

namespace ROOT {
   static TClass *vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_Dictionary();
   static void vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_TClassManip(TClass*);
   static void *new_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p = 0);
   static void *newArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(Long_t size, void *p);
   static void delete_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p);
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p);
   static void destruct_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<vector<vector<vector<vector<float> > > > >*)
   {
      vector<vector<vector<vector<vector<float> > > > > *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<vector<vector<vector<vector<float> > > > >));
      static ::ROOT::TGenericClassInfo 
         instance("vector<vector<vector<vector<vector<float> > > > >", -2, "vector", 214,
                  typeid(vector<vector<vector<vector<vector<float> > > > >), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<vector<vector<vector<vector<float> > > > >) );
      instance.SetNew(&new_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR);
      instance.SetNewArray(&newArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR);
      instance.SetDelete(&delete_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR);
      instance.SetDeleteArray(&deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR);
      instance.SetDestructor(&destruct_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<vector<vector<vector<vector<float> > > > > >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<vector<vector<vector<vector<float> > > > >*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<vector<vector<vector<vector<float> > > > >*)0x0)->GetClass();
      vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<vector<float> > > > > : new vector<vector<vector<vector<vector<float> > > > >;
   }
   static void *newArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<vector<float> > > > >[nElements] : new vector<vector<vector<vector<vector<float> > > > >[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p) {
      delete ((vector<vector<vector<vector<vector<float> > > > >*)p);
   }
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p) {
      delete [] ((vector<vector<vector<vector<vector<float> > > > >*)p);
   }
   static void destruct_vectorlEvectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgRsPgR(void *p) {
      typedef vector<vector<vector<vector<vector<float> > > > > current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<vector<vector<vector<vector<float> > > > >

namespace ROOT {
   static TClass *vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_Dictionary();
   static void vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_TClassManip(TClass*);
   static void *new_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p = 0);
   static void *newArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(Long_t size, void *p);
   static void delete_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p);
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p);
   static void destruct_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<vector<vector<vector<float> > > >*)
   {
      vector<vector<vector<vector<float> > > > *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<vector<vector<vector<float> > > >));
      static ::ROOT::TGenericClassInfo 
         instance("vector<vector<vector<vector<float> > > >", -2, "vector", 214,
                  typeid(vector<vector<vector<vector<float> > > >), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<vector<vector<vector<float> > > >) );
      instance.SetNew(&new_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR);
      instance.SetNewArray(&newArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR);
      instance.SetDelete(&delete_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR);
      instance.SetDeleteArray(&deleteArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR);
      instance.SetDestructor(&destruct_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<vector<vector<vector<float> > > > >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<vector<vector<vector<float> > > >*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<vector<vector<vector<float> > > >*)0x0)->GetClass();
      vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<float> > > > : new vector<vector<vector<vector<float> > > >;
   }
   static void *newArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<vector<float> > > >[nElements] : new vector<vector<vector<vector<float> > > >[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p) {
      delete ((vector<vector<vector<vector<float> > > >*)p);
   }
   static void deleteArray_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p) {
      delete [] ((vector<vector<vector<vector<float> > > >*)p);
   }
   static void destruct_vectorlEvectorlEvectorlEvectorlEfloatgRsPgRsPgRsPgR(void *p) {
      typedef vector<vector<vector<vector<float> > > > current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<vector<vector<vector<float> > > >

namespace ROOT {
   static TClass *vectorlEvectorlEvectorlEfloatgRsPgRsPgR_Dictionary();
   static void vectorlEvectorlEvectorlEfloatgRsPgRsPgR_TClassManip(TClass*);
   static void *new_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p = 0);
   static void *newArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(Long_t size, void *p);
   static void delete_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p);
   static void deleteArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p);
   static void destruct_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<vector<vector<float> > >*)
   {
      vector<vector<vector<float> > > *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<vector<vector<float> > >));
      static ::ROOT::TGenericClassInfo 
         instance("vector<vector<vector<float> > >", -2, "vector", 214,
                  typeid(vector<vector<vector<float> > >), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEvectorlEvectorlEfloatgRsPgRsPgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<vector<vector<float> > >) );
      instance.SetNew(&new_vectorlEvectorlEvectorlEfloatgRsPgRsPgR);
      instance.SetNewArray(&newArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR);
      instance.SetDelete(&delete_vectorlEvectorlEvectorlEfloatgRsPgRsPgR);
      instance.SetDeleteArray(&deleteArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR);
      instance.SetDestructor(&destruct_vectorlEvectorlEvectorlEfloatgRsPgRsPgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<vector<vector<float> > > >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<vector<vector<float> > >*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEvectorlEvectorlEfloatgRsPgRsPgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<vector<vector<float> > >*)0x0)->GetClass();
      vectorlEvectorlEvectorlEfloatgRsPgRsPgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEvectorlEvectorlEfloatgRsPgRsPgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<float> > > : new vector<vector<vector<float> > >;
   }
   static void *newArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<vector<float> > >[nElements] : new vector<vector<vector<float> > >[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p) {
      delete ((vector<vector<vector<float> > >*)p);
   }
   static void deleteArray_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p) {
      delete [] ((vector<vector<vector<float> > >*)p);
   }
   static void destruct_vectorlEvectorlEvectorlEfloatgRsPgRsPgR(void *p) {
      typedef vector<vector<vector<float> > > current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<vector<vector<float> > >

namespace ROOT {
   static TClass *vectorlEvectorlEfloatgRsPgR_Dictionary();
   static void vectorlEvectorlEfloatgRsPgR_TClassManip(TClass*);
   static void *new_vectorlEvectorlEfloatgRsPgR(void *p = 0);
   static void *newArray_vectorlEvectorlEfloatgRsPgR(Long_t size, void *p);
   static void delete_vectorlEvectorlEfloatgRsPgR(void *p);
   static void deleteArray_vectorlEvectorlEfloatgRsPgR(void *p);
   static void destruct_vectorlEvectorlEfloatgRsPgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<vector<float> >*)
   {
      vector<vector<float> > *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<vector<float> >));
      static ::ROOT::TGenericClassInfo 
         instance("vector<vector<float> >", -2, "vector", 214,
                  typeid(vector<vector<float> >), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEvectorlEfloatgRsPgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<vector<float> >) );
      instance.SetNew(&new_vectorlEvectorlEfloatgRsPgR);
      instance.SetNewArray(&newArray_vectorlEvectorlEfloatgRsPgR);
      instance.SetDelete(&delete_vectorlEvectorlEfloatgRsPgR);
      instance.SetDeleteArray(&deleteArray_vectorlEvectorlEfloatgRsPgR);
      instance.SetDestructor(&destruct_vectorlEvectorlEfloatgRsPgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<vector<float> > >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<vector<float> >*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEvectorlEfloatgRsPgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<vector<float> >*)0x0)->GetClass();
      vectorlEvectorlEfloatgRsPgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEvectorlEfloatgRsPgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEvectorlEfloatgRsPgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<float> > : new vector<vector<float> >;
   }
   static void *newArray_vectorlEvectorlEfloatgRsPgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<vector<float> >[nElements] : new vector<vector<float> >[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEvectorlEfloatgRsPgR(void *p) {
      delete ((vector<vector<float> >*)p);
   }
   static void deleteArray_vectorlEvectorlEfloatgRsPgR(void *p) {
      delete [] ((vector<vector<float> >*)p);
   }
   static void destruct_vectorlEvectorlEfloatgRsPgR(void *p) {
      typedef vector<vector<float> > current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<vector<float> >

namespace ROOT {
   static TClass *vectorlEfloatgR_Dictionary();
   static void vectorlEfloatgR_TClassManip(TClass*);
   static void *new_vectorlEfloatgR(void *p = 0);
   static void *newArray_vectorlEfloatgR(Long_t size, void *p);
   static void delete_vectorlEfloatgR(void *p);
   static void deleteArray_vectorlEfloatgR(void *p);
   static void destruct_vectorlEfloatgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<float>*)
   {
      vector<float> *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<float>));
      static ::ROOT::TGenericClassInfo 
         instance("vector<float>", -2, "vector", 214,
                  typeid(vector<float>), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEfloatgR_Dictionary, isa_proxy, 0,
                  sizeof(vector<float>) );
      instance.SetNew(&new_vectorlEfloatgR);
      instance.SetNewArray(&newArray_vectorlEfloatgR);
      instance.SetDelete(&delete_vectorlEfloatgR);
      instance.SetDeleteArray(&deleteArray_vectorlEfloatgR);
      instance.SetDestructor(&destruct_vectorlEfloatgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<float> >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<float>*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEfloatgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<float>*)0x0)->GetClass();
      vectorlEfloatgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEfloatgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEfloatgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<float> : new vector<float>;
   }
   static void *newArray_vectorlEfloatgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<float>[nElements] : new vector<float>[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEfloatgR(void *p) {
      delete ((vector<float>*)p);
   }
   static void deleteArray_vectorlEfloatgR(void *p) {
      delete [] ((vector<float>*)p);
   }
   static void destruct_vectorlEfloatgR(void *p) {
      typedef vector<float> current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<float>

namespace ROOT {
   static TClass *vectorlEMELAParticlemUgR_Dictionary();
   static void vectorlEMELAParticlemUgR_TClassManip(TClass*);
   static void *new_vectorlEMELAParticlemUgR(void *p = 0);
   static void *newArray_vectorlEMELAParticlemUgR(Long_t size, void *p);
   static void delete_vectorlEMELAParticlemUgR(void *p);
   static void deleteArray_vectorlEMELAParticlemUgR(void *p);
   static void destruct_vectorlEMELAParticlemUgR(void *p);

   // Function generating the singleton type initializer
   static TGenericClassInfo *GenerateInitInstanceLocal(const vector<MELAParticle*>*)
   {
      vector<MELAParticle*> *ptr = 0;
      static ::TVirtualIsAProxy* isa_proxy = new ::TIsAProxy(typeid(vector<MELAParticle*>));
      static ::ROOT::TGenericClassInfo 
         instance("vector<MELAParticle*>", -2, "vector", 214,
                  typeid(vector<MELAParticle*>), ::ROOT::Internal::DefineBehavior(ptr, ptr),
                  &vectorlEMELAParticlemUgR_Dictionary, isa_proxy, 4,
                  sizeof(vector<MELAParticle*>) );
      instance.SetNew(&new_vectorlEMELAParticlemUgR);
      instance.SetNewArray(&newArray_vectorlEMELAParticlemUgR);
      instance.SetDelete(&delete_vectorlEMELAParticlemUgR);
      instance.SetDeleteArray(&deleteArray_vectorlEMELAParticlemUgR);
      instance.SetDestructor(&destruct_vectorlEMELAParticlemUgR);
      instance.AdoptCollectionProxyInfo(TCollectionProxyInfo::Generate(TCollectionProxyInfo::Pushback< vector<MELAParticle*> >()));
      return &instance;
   }
   // Static variable to force the class initialization
   static ::ROOT::TGenericClassInfo *_R__UNIQUE_DICT_(Init) = GenerateInitInstanceLocal((const vector<MELAParticle*>*)0x0); R__UseDummy(_R__UNIQUE_DICT_(Init));

   // Dictionary for non-ClassDef classes
   static TClass *vectorlEMELAParticlemUgR_Dictionary() {
      TClass* theClass =::ROOT::GenerateInitInstanceLocal((const vector<MELAParticle*>*)0x0)->GetClass();
      vectorlEMELAParticlemUgR_TClassManip(theClass);
   return theClass;
   }

   static void vectorlEMELAParticlemUgR_TClassManip(TClass* ){
   }

} // end of namespace ROOT

namespace ROOT {
   // Wrappers around operator new
   static void *new_vectorlEMELAParticlemUgR(void *p) {
      return  p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<MELAParticle*> : new vector<MELAParticle*>;
   }
   static void *newArray_vectorlEMELAParticlemUgR(Long_t nElements, void *p) {
      return p ? ::new((::ROOT::Internal::TOperatorNewHelper*)p) vector<MELAParticle*>[nElements] : new vector<MELAParticle*>[nElements];
   }
   // Wrapper around operator delete
   static void delete_vectorlEMELAParticlemUgR(void *p) {
      delete ((vector<MELAParticle*>*)p);
   }
   static void deleteArray_vectorlEMELAParticlemUgR(void *p) {
      delete [] ((vector<MELAParticle*>*)p);
   }
   static void destruct_vectorlEMELAParticlemUgR(void *p) {
      typedef vector<MELAParticle*> current_t;
      ((current_t*)p)->~current_t();
   }
} // end of namespace ROOT for class vector<MELAParticle*>

namespace {
  void TriggerDictionaryInitialization_LinkDef_out_Impl() {
    static const char* headers[] = {
0
    };
    static const char* includePaths[] = {
"./interface",
"/cvmfs/cms.cern.ch/slc6_amd64_gcc630/lcg/root/6.10.04-ghjeda//include",
"/cvmfs/cms.cern.ch/slc6_amd64_gcc630/lcg/root/6.10.04-ghjeda/include",
"/afs/cern.ch/work/q/qguo/public/CMSSW_9_3_0/src/makegridpacks/workdir/ggZH_4l/ggZH_NNPDF31_13TeV_M125/ggZH4l125_JHUGen/JHUGenMELA/MELA/",
0
    };
    static const char* fwdDeclCode = R"DICTFWDDCLS(
#line 1 "LinkDef_out dictionary forward declarations' payload"
#pragma clang diagnostic ignored "-Wkeyword-compat"
#pragma clang diagnostic ignored "-Wignored-attributes"
#pragma clang diagnostic ignored "-Wreturn-type-c-linkage"
extern int __Cling_Autoloading_Map;
class __attribute__((annotate("$clingAutoload$MELAParticle.h")))  __attribute__((annotate("$clingAutoload$Mela.h")))  MELAParticle;
namespace std{template <typename _Tp> class __attribute__((annotate("$clingAutoload$bits/allocator.h")))  __attribute__((annotate("$clingAutoload$string")))  allocator;
}
class __attribute__((annotate("$clingAutoload$MELALinearInterpFunc.h")))  MELALinearInterpFunc;
class __attribute__((annotate("$clingAutoload$MELANCSplineCore.h")))  __attribute__((annotate("$clingAutoload$MELANCSplineFactory_1D.h")))  MELANCSplineCore;
class __attribute__((annotate("$clingAutoload$MELANCSpline_1D_fast.h")))  __attribute__((annotate("$clingAutoload$MELANCSplineFactory_1D.h")))  MELANCSpline_1D_fast;
class __attribute__((annotate("$clingAutoload$MELAFuncPdf.h")))  __attribute__((annotate("$clingAutoload$MELANCSplineFactory_1D.h")))  MELAFuncPdf;
class __attribute__((annotate("$clingAutoload$MELANCSplineFactory_1D.h")))  MELANCSplineFactory_1D;
class __attribute__((annotate("$clingAutoload$MELANCSpline_2D_fast.h")))  __attribute__((annotate("$clingAutoload$MELANCSplineFactory_2D.h")))  MELANCSpline_2D_fast;
class __attribute__((annotate("$clingAutoload$MELANCSplineFactory_2D.h")))  MELANCSplineFactory_2D;
class __attribute__((annotate("$clingAutoload$MELANCSpline_3D_fast.h")))  __attribute__((annotate("$clingAutoload$MELANCSplineFactory_3D.h")))  MELANCSpline_3D_fast;
class __attribute__((annotate("$clingAutoload$MELANCSplineFactory_3D.h")))  MELANCSplineFactory_3D;
class __attribute__((annotate("$clingAutoload$TCouplings.hh")))  __attribute__((annotate("$clingAutoload$Mela.h")))  SpinZeroCouplings;
class __attribute__((annotate("$clingAutoload$TCouplings.hh")))  __attribute__((annotate("$clingAutoload$Mela.h")))  SpinOneCouplings;
class __attribute__((annotate("$clingAutoload$TCouplings.hh")))  __attribute__((annotate("$clingAutoload$Mela.h")))  SpinTwoCouplings;
class __attribute__((annotate("$clingAutoload$TCouplings.hh")))  __attribute__((annotate("$clingAutoload$Mela.h")))  VprimeCouplings;
class __attribute__((annotate("$clingAutoload$MELATopCandidate.h")))  __attribute__((annotate("$clingAutoload$Mela.h")))  MELATopCandidate;
class __attribute__((annotate("$clingAutoload$MELACandidate.h")))  __attribute__((annotate("$clingAutoload$Mela.h")))  MELACandidate;
class __attribute__((annotate("$clingAutoload$MelaIO.h")))  __attribute__((annotate("$clingAutoload$Mela.h")))  MelaIO;
class __attribute__((annotate("$clingAutoload$TEvtProb.hh")))  __attribute__((annotate("$clingAutoload$Mela.h")))  TEvtProb;
class __attribute__((annotate("$clingAutoload$Mela.h")))  Mela;
)DICTFWDDCLS";
    static const char* payloadCode = R"DICTPAYLOAD(
#line 1 "LinkDef_out dictionary payload"

#ifndef G__VECTOR_HAS_CLASS_ITERATOR
  #define G__VECTOR_HAS_CLASS_ITERATOR 1
#endif

#define _BACKWARD_BACKWARD_WARNING_H
#include "MELALinearInterpFunc.h"
#include "MELANCSplineFactory_1D.h"
#include "MELANCSplineFactory_2D.h"
#include "MELANCSplineFactory_3D.h"
#include "MELAFuncPdf.h"
#include "Mela.h"

#ifdef __CINT__


#pragma link off all globals;
#pragma link off all classes;
#pragma link off all functions;

#pragma link C++ nestedclasses;
#pragma link C++ nestedtypedefs;

#pragma link C++ class MELAParticle;
#pragma link C++ class std::vector<MELAParticle*>;
#pragma link C++ class MELATopCandidate;
#pragma link C++ class MELACandidate;
#pragma link C++ class MelaIO;
#pragma link C++ namespace TVar;

#pragma link C++ class MELAFuncPdf;
#pragma link C++ class MELALinearInterpFunc;
#pragma link C++ class MELANCSplineCore;
#pragma link C++ class MELANCSpline_1D_fast;
#pragma link C++ class MELANCSpline_2D_fast;
#pragma link C++ class MELANCSpline_3D_fast;
#pragma link C++ class MELANCSplineFactory_1D;
#pragma link C++ class MELANCSplineFactory_2D;
#pragma link C++ class MELANCSplineFactory_3D;

#pragma link C++ namespace TUtil;
#pragma link C++ function TUtil::computeAngles;
#pragma link C++ function TUtil::computeAnglesCS;
#pragma link C++ function TUtil::computeVBFAngles;
#pragma link C++ function TUtil::computeVBFAngles_ComplexBoost;
#pragma link C++ function TUtil::computeVHAngles;
#pragma link C++ function TUtil::scaleMomentumToEnergy;
#pragma link C++ function TUtil::constrainedRemovePairMass;
#pragma link C++ function TUtil::removeMassFromPair;
#pragma link C++ function TUtil::adjustTopDaughters;
#pragma link C++ function TUtil::computeFakeJet;
//
#pragma link C++ function TUtil::SetAlphaS;
#pragma link C++ function TUtil::CheckPartonMomFraction;
#pragma link C++ function TUtil::ComputePDF;
#pragma link C++ function TUtil::SumMEPDF;
//
#pragma link C++ function TUtil::GetBoostedParticleVectors;
#pragma link C++ function TUtil::ConvertVectorFormat;
#pragma link C++ function TUtil::ConvertTopCandidate;
//
#pragma link C++ function TUtil::SumMatrixElementPDF;
#pragma link C++ function TUtil::JHUGenMatEl;
#pragma link C++ function TUtil::HJJMatEl;
#pragma link C++ function TUtil::VHiggsMatEl;
#pragma link C++ function TUtil::TTHiggsMatEl;
#pragma link C++ function TUtil::BBHiggsMatEl;

#pragma link C++ class SpinZeroCouplings;
#pragma link C++ class SpinOneCouplings;
#pragma link C++ class SpinTwoCouplings;
#pragma link C++ class VprimeCouplings;
#pragma link C++ class TEvtProb;
#pragma link C++ class ZZMatrixElement;
#pragma link C++ class Mela;


#endif

#undef  _BACKWARD_BACKWARD_WARNING_H
)DICTPAYLOAD";
    static const char* classesHeaders[]={
"MELACandidate", payloadCode, "@",
"MELAFuncPdf", payloadCode, "@",
"MELALinearInterpFunc", payloadCode, "@",
"MELANCSplineCore", payloadCode, "@",
"MELANCSplineFactory_1D", payloadCode, "@",
"MELANCSplineFactory_2D", payloadCode, "@",
"MELANCSplineFactory_3D", payloadCode, "@",
"MELANCSpline_1D_fast", payloadCode, "@",
"MELANCSpline_2D_fast", payloadCode, "@",
"MELANCSpline_3D_fast", payloadCode, "@",
"MELAParticle", payloadCode, "@",
"MELATopCandidate", payloadCode, "@",
"Mela", payloadCode, "@",
"MelaIO", payloadCode, "@",
"SpinOneCouplings", payloadCode, "@",
"SpinTwoCouplings", payloadCode, "@",
"SpinZeroCouplings", payloadCode, "@",
"TEvtProb", payloadCode, "@",
"TUtil::BBHiggsMatEl", payloadCode, "@",
"TUtil::CheckPartonMomFraction", payloadCode, "@",
"TUtil::ComputePDF", payloadCode, "@",
"TUtil::ConvertTopCandidate", payloadCode, "@",
"TUtil::ConvertVectorFormat", payloadCode, "@",
"TUtil::GetBoostedParticleVectors", payloadCode, "@",
"TUtil::HJJMatEl", payloadCode, "@",
"TUtil::JHUGenMatEl", payloadCode, "@",
"TUtil::SetAlphaS", payloadCode, "@",
"TUtil::SumMEPDF", payloadCode, "@",
"TUtil::SumMatrixElementPDF", payloadCode, "@",
"TUtil::TTHiggsMatEl", payloadCode, "@",
"TUtil::VHiggsMatEl", payloadCode, "@",
"TUtil::adjustTopDaughters", payloadCode, "@",
"TUtil::computeAngles", payloadCode, "@",
"TUtil::computeAnglesCS", payloadCode, "@",
"TUtil::computeFakeJet", payloadCode, "@",
"TUtil::computeVBFAngles", payloadCode, "@",
"TUtil::computeVBFAngles_ComplexBoost", payloadCode, "@",
"TUtil::computeVHAngles", payloadCode, "@",
"TUtil::constrainedRemovePairMass", payloadCode, "@",
"TUtil::removeMassFromPair", payloadCode, "@",
"TUtil::scaleMomentumToEnergy", payloadCode, "@",
"VprimeCouplings", payloadCode, "@",
nullptr};

    static bool isInitialized = false;
    if (!isInitialized) {
      TROOT::RegisterModule("LinkDef_out",
        headers, includePaths, payloadCode, fwdDeclCode,
        TriggerDictionaryInitialization_LinkDef_out_Impl, {}, classesHeaders);
      isInitialized = true;
    }
  }
  static struct DictInit {
    DictInit() {
      TriggerDictionaryInitialization_LinkDef_out_Impl();
    }
  } __TheDictionaryInitializer;
}
void TriggerDictionaryInitialization_LinkDef_out() {
  TriggerDictionaryInitialization_LinkDef_out_Impl();
}
