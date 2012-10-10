﻿// Microsoft Research License Agreement
// Non-Commercial Use Only 
// Azure Library for Lucene.Net
// _____________________________________________________________________
// This Microsoft Research License Agreement, including all exhibits ("MSR-LA") is a legal agreement between you and Microsoft Corporation 
// (“Microsoft” or “we”) for the software or data identified above, which may include source code, and any associated materials, text or speech 
// files, associated media and "online" or electronic documentation and any updates we provide in our discretion (together, the "Software"). 
//
// By installing, copying, or otherwise using this Software, found at http:// research.microsoft.com/downloads, you agree to be bound by the terms of 
// this MSR-LA.  If you do not agree, do not install copy or use the Software. The Software is protected by copyright and other intellectual 
// property laws and is licensed, not sold.    
//
// SCOPE OF RIGHTS:
// You may use, copy, reproduce, and distribute this Software for any non-commercial purpose, subject to the restrictions in this MSR-LA. Some 
// purposes which can be non-commercial are teaching, academic research, public demonstrations and personal experimentation. You may also distribute 
// this Software with books or other teaching materials, or publish the Software on websites, that are intended to teach the use of the Software for 
// academic or other non-commercial purposes.
//
// You may not use or distribute this Software or any derivative works in any form for commercial purposes. Examples of commercial purposes would be 
// running business operations, licensing, leasing, or selling the Software, distributing the Software for use with commercial products, using the 
// Software in the creation or use of commercial products or any other activity which purpose is to procure a commercial gain to you or others.
// If the Software includes source code or data, you may create derivative works of such portions of the Software and distribute the modified 
// Software for non-commercial purposes, as provided herein.  
//
// If you distribute the Software or any derivative works of the Software, you will distribute them under the same terms and conditions as in this 
// license, and you will not grant other rights to the Software or derivative works that are different from those provided by this MSR-LA. 
// If you have created derivative works of the Software, and distribute such derivative works, you will cause the modified files to carry prominent 
// notices so that recipients know that they are not receiving the original Software. Such notices must state: (i) that you have changed the 
// Software; and (ii) the date of any changes.
//
// In return, we simply require that you agree: 
// 1.	That you will not remove any copyright or other notices from the Software.
// 2.	That if any of the Software is in binary format, you will not attempt to modify such portions of the Software, or to reverse engineer or 
// decompile them, except and only to the extent authorized by applicable law. 
// 3.	That Microsoft is granted back, without any restrictions or limitations, a non-exclusive, perpetual, irrevocable, royalty-free, assignable 
// and sub-licensable license, to reproduce, publicly perform or display, install, use, modify, post, distribute, make and have made, sell and 
// transfer your modifications to and/or derivative works of the Software source code or data, for any purpose.  
// 4.	That any feedback about the Software provided by you to us is voluntarily given, and Microsoft shall be free to use the feedback as it sees 
// fit without obligation or restriction of any kind, even if the feedback is designated by you as confidential. 
// 5.	THAT THE SOFTWARE COMES "AS IS", WITH NO WARRANTIES. THIS MEANS NO EXPRESS, IMPLIED OR STATUTORY WARRANTY, INCLUDING WITHOUT LIMITATION, 
// WARRANTIES OF MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE, ANY WARRANTY AGAINST INTERFERENCE WITH YOUR ENJOYMENT OF THE SOFTWARE OR ANY 
// WARRANTY OF TITLE OR NON-INFRINGEMENT. THERE IS NO WARRANTY THAT THIS SOFTWARE WILL FULFILL ANY OF YOUR PARTICULAR PURPOSES OR NEEDS. ALSO, YOU 
// MUST PASS THIS DISCLAIMER ON WHENEVER YOU DISTRIBUTE THE SOFTWARE OR DERIVATIVE WORKS.
// 6.	THAT NEITHER MICROSOFT NOR ANY CONTRIBUTOR TO THE SOFTWARE WILL BE LIABLE FOR ANY DAMAGES RELATED TO THE SOFTWARE OR THIS MSR-LA, INCLUDING 
// DIRECT, INDIRECT, SPECIAL, CONSEQUENTIAL OR INCIDENTAL DAMAGES, TO THE MAXIMUM EXTENT THE LAW PERMITS, NO MATTER WHAT LEGAL THEORY IT IS BASED 
// ON. ALSO, YOU MUST PASS THIS LIMITATION OF LIABILITY ON WHENEVER YOU DISTRIBUTE THE SOFTWARE OR DERIVATIVE WORKS.
// 7.	That we have no duty of reasonable care or lack of negligence, and we are not obligated to (and will not) provide technical support for the 
// Software.
// 8.	That if you breach this MSR-LA or if you sue anyone over patents that you think may apply to or read on the Software or anyone's use of the 
// Software, this MSR-LA (and your license and rights obtained herein) terminate automatically.  Upon any such termination, you shall destroy all of 
// your copies of the Software immediately.  Sections 3, 4, 5, 6, 7, 8, 11 and 12 of this MSR-LA shall survive any termination of this MSR-LA.
// 9.	That the patent rights, if any, granted to you in this MSR-LA only apply to the Software, not to any derivative works you make.
// 10.	That the Software may be subject to U.S. export jurisdiction at the time it is licensed to you, and it may be subject to additional export or 
// import laws in other places.  You agree to comply with all such laws and regulations that may apply to the Software after delivery of the 
// software to you.
// 11.	That all rights not expressly granted to you in this MSR-LA are reserved.
// 12.	That this MSR-LA shall be construed and controlled by the laws of the State of Washington, USA, without regard to conflicts of law.  If any 
// provision of this MSR-LA shall be deemed unenforceable or contrary to law, the rest of this MSR-LA shall remain in full effect and interpreted in 
// an enforceable manner that most nearly captures the intent of the original language. 
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.Compression;
using System.IO;

namespace Lucene.Net.Store.Azure
{
    // class which can be used to compress fields in documents
    // to use will need to define the ICompressionAdapter for Lucene so that it is loaded by this code in Lucene
    // <summary>
    //  private static void CheckCompressionSupport()
    //{
    //    if (compressionAdapter == null)
    //    {
    //        System.String compressionLibClassName = SupportClass.AppSettings.Get("Lucene.Net.CompressionLib.class", null);
    //        if (compressionLibClassName == null)
    //            throw new System.SystemException("Compression support not configured"); 

    //        Type compressionLibClass = Type.GetType(compressionLibClassName, true);
    //        System.Object compressionAdapterObj = Activator.CreateInstance(compressionLibClass);
    //        compressionAdapter = compressionAdapterObj as ICompressionAdapter;
    //        if (compressionAdapter == null)
    //            throw new System.SystemException("Compression adapter does not support the ICompressionAdapter interface");
    //    }
    //}
    // </summary>
    public class DeflateAdapter : SupportClass.CompressionSupport.ICompressionAdapter
    {
        public byte[] Compress(byte[] input)
        {
            MemoryStream outputStream = new MemoryStream();
            DeflateStream compressor = new DeflateStream(outputStream, CompressionMode.Compress);
            compressor.Write(input, 0, input.Length);
            compressor.Close();
            return outputStream.ToArray();
        }

        public byte[] Uncompress(byte[] input)
        {
            MemoryStream outputStream = new MemoryStream();
            DeflateStream decompressor = new DeflateStream(outputStream, CompressionMode.Decompress);
            decompressor.Write(input, 0, input.Length);
            decompressor.Close();
            return outputStream.ToArray();
        }
    }
}
