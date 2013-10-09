Imports Masterchest.mlib
Imports System.Data.SqlClient

Module Engine
    Dim debuglevel As Integer = 1
    Dim pubkeyhex As String
    Dim sqlcon As String

    Sub Main()
        'Create a bitcoind/qt connection:
        Dim bitcoin_con As New bitcoinrpcconnection
        Dim sqlserv, sqldata, sqluser, sqlpass As String

        'get parameters
        Dim strStartupArguments() As String, intCount As Integer
        strStartupArguments = System.Environment.GetCommandLineArgs
        For intCount = 0 To UBound(strStartupArguments)
            If Len(strStartupArguments(intCount)) > 16 Then
                Select Case strStartupArguments(intCount).ToLower.Substring(0, 16)
                    Case "-bitcoinrpcserv="
                        bitcoin_con.bitcoinrpcserver = strStartupArguments(intCount).ToLower.Substring(16, Len(strStartupArguments(intCount)) - 16)
                    Case "-bitcoinrpcport="
                        bitcoin_con.bitcoinrpcport = Val(strStartupArguments(intCount).ToLower.Substring(16, Len(strStartupArguments(intCount)) - 16))
                    Case "-bitcoinrpcuser="
                        bitcoin_con.bitcoinrpcuser = strStartupArguments(intCount).ToLower.Substring(16, Len(strStartupArguments(intCount)) - 16)
                    Case "-bitcoinrpcpass="
                        bitcoin_con.bitcoinrpcpassword = strStartupArguments(intCount).ToLower.Substring(16, Len(strStartupArguments(intCount)) - 16)
                End Select
            End If
            If Len(strStartupArguments(intCount)) > 9 Then
                Select Case strStartupArguments(intCount).ToLower.Substring(0, 9)
                    Case "-sqlserv="
                        sqlserv = strStartupArguments(intCount).ToLower.Substring(9, Len(strStartupArguments(intCount)) - 9)
                    Case "-sqldata="
                        sqldata = strStartupArguments(intCount).ToLower.Substring(9, Len(strStartupArguments(intCount)) - 9)
                    Case "-sqluser="
                        sqluser = strStartupArguments(intCount).ToLower.Substring(9, Len(strStartupArguments(intCount)) - 9)
                    Case "-sqlpass="
                        sqlpass = strStartupArguments(intCount).ToLower.Substring(9, Len(strStartupArguments(intCount)) - 9)
                End Select
            End If
        Next intCount

        If sqlserv = "" Or sqldata = "" Or sqluser = "" Or sqlpass = "" Or bitcoin_con.bitcoinrpcpassword = "" Or bitcoin_con.bitcoinrpcport = 0 Or bitcoin_con.bitcoinrpcserver = "" Or bitcoin_con.bitcoinrpcuser = "" Then
            Console.WriteLine("Masterchest Engine v0.1a")
            Console.WriteLine("Could not correctly parse arguments - exiting...")
            End
        End If

        'setup sql connection string
        sqlcon = "data source=" & sqlserv & ";initial catalog=" & sqldata & ";User ID=" & sqluser & ";Password=" & sqlpass
        Console.WriteLine("Masterchest Engine v0.1a")
        Console.WriteLine("Starting...")
        Console.WriteLine()

        'Create a new mlib instance
        Dim mlib As New Masterchest.mlib

        'test connection to bitcoind
        Dim checkhash As blockhash = mlib.getblockhash(bitcoin_con, 2)
        If checkhash.result.ToString = "000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd" Then 'we've got a correct response
            Console.WriteLine("STATUS: Connection to bitcoin RPC established & sanity check OK.")
        Else
            'something has gone wrong
            Console.WriteLine("ERROR: Connection to bitcoin RPC seems to be established but responses are not as expected." & vbCrLf & "Exiting...")
            End
        End If

        'test connection to database
        Dim testval As Integer
        testval = SQLGetSingleVal("SELECT count(*) FROM information_schema.columns WHERE table_name = 'processedblocks'")
        If testval = 2 Then 'sanity check ok
            Console.WriteLine("STATUS: Connection to database established & sanity check OK.")
        Else
            'something has gone wrong
            Console.WriteLine("ERROR: Connection to database seems to be established but responses are not as expected." & vbCrLf & "Exiting...")
            End
        End If

        '### we have confirmed our connections to resources external to the program ###
        'check processedblocks for last database block and update from there - always delete current last block transactions and go back one ensuring we don't miss transactions if code bombs while processing a block
        Dim dbposition As Integer
        dbposition = SQLGetSingleVal("SELECT MAX(BLOCKNUM) FROM processedblocks")
        If dbposition > 249497 Then dbposition = dbposition - 1 Else dbposition = 249497 'roll database back one block for safety
        'delete transactions after dbposition block
        Dim txdeletedcount = SQLGetSingleVal("DELETE FROM transactions WHERE BLOCKNUM > " & dbposition - 1)
        Dim blockdeletedcount = SQLGetSingleVal("DELETE FROM processedblocks WHERE BLOCKNUM > " & dbposition - 1)
        Console.WriteLine("STATUS: Database starting at block " & dbposition.ToString)
        'check bitcoin RPC for latest block
        Dim rpcblock As Integer
        Dim blockcount As blockcount = mlib.getblockcount(bitcoin_con)
        rpcblock = blockcount.result
        Console.WriteLine("STATUS: Network is at block " & rpcblock.ToString)

        'calculate catchup
        Dim catchup As Integer
        catchup = rpcblock - dbposition
        Console.WriteLine("STATUS: " & catchup.ToString & " blocks to catch up")

        '### loop through blocks since dbposition and add any transactions detected as mastercoin to the transactions table
        Dim msctranscount As Integer
        msctranscount = 0
        Dim msctrans(100000) As String 'support up to 100000 transactions initiailly
        For x = dbposition To rpcblock
            Dim blocknum As Integer = x
            If debuglevel > 0 Then Console.WriteLine("DEBUG: Block Analysis for: " & blocknum.ToString)
            Dim blockhash As blockhash = mlib.getblockhash(bitcoin_con, blocknum)
            Dim block As Block = mlib.getblock(bitcoin_con, blockhash.result.ToString)
            Dim txarray() As String = block.result.tx.ToArray

            For j = 1 To UBound(txarray) 'skip tx0 which should be coinbase
                Try
                    If mlib.ismastercointx(bitcoin_con, txarray(j)) = True Then
                        Console.WriteLine("BLOCKSCAN: Found MSC transaction: " & txarray(j))
                        Dim results As txn = mlib.gettransaction(bitcoin_con, txarray(j))
                        'handle generate
                        If results.result.blocktime < 1377993875 Then 'before exodus cutofff
                            Dim mastercointxinfo As mastercointx = mlib.getmastercointransaction(bitcoin_con, txarray(j).ToString, "generate")
                            If mastercointxinfo.type = "generate" And mastercointxinfo.curtype = 0 Then
                                Dim dbwritemsc As Integer = SQLGetSingleVal("INSERT INTO transactions VALUES ('" & mastercointxinfo.txid & "','" & mastercointxinfo.fromadd & "','" & mastercointxinfo.toadd & "'," & mastercointxinfo.value & ",'" & mastercointxinfo.type & "'," & mastercointxinfo.blocktime & "," & blocknum & "," & mastercointxinfo.valid & ",1)")
                                Dim dbwritetmsc As Integer = SQLGetSingleVal("INSERT INTO transactions VALUES ('" & mastercointxinfo.txid & "','" & mastercointxinfo.fromadd & "','" & mastercointxinfo.toadd & "'," & mastercointxinfo.value & ",'" & mastercointxinfo.type & "'," & mastercointxinfo.blocktime & "," & blocknum & "," & mastercointxinfo.valid & ",2)")
                            End If
                        End If
                        'decode mastercoin transaction
                        Dim txdetails As mastercointx = mlib.getmastercointransaction(bitcoin_con, txarray(j).ToString, "send")
                        'see if we have a transaction back and if so write it to database
                        If Not IsNothing(txdetails) Then Dim dbwrite2 As Integer = SQLGetSingleVal("INSERT INTO transactions VALUES ('" & txdetails.txid & "','" & txdetails.fromadd & "','" & txdetails.toadd & "'," & txdetails.value & ",'" & txdetails.type & "'," & txdetails.blocktime & "," & blocknum & "," & txdetails.valid & "," & txdetails.curtype & ")")
                    End If
                Catch e As Exception
                    Console.WriteLine("ERROR: Exception occured." & vbCrLf & e.Message & vbCrLf & "Exiting...")
                End Try
            Next

            'only here do we write that the block has been processed to database
            Dim dbwrite3 As Integer = SQLGetSingleVal("INSERT INTO processedblocks VALUES (" & blocknum & "," & block.result.time & ")")
        Next

        'finished scanning, next process transactions
        processtx()

    End Sub



    '////////////
    '///FUNCTIONS
    '////////////
    Public Function processtx()
        Console.WriteLine("Processing transactions")
        'do all generate transactions and calculate initial balances
        Dim con2 As New SqlClient.SqlConnection(sqlcon)
        Dim cmd2 As New SqlCommand()
        cmd2.Connection = con2
        con2.Open()
        cmd2.CommandText = "delete from transactions_processed"
        cmd2.ExecuteNonQuery()
        cmd2.CommandText = "delete from balances"
        cmd2.ExecuteNonQuery()
        cmd2.CommandText = "insert into transactions_processed (TXID,FROMADD,TOADD,VALUE,TYPE,BLOCKTIME,BLOCKNUM,VALID,CURTYPE) SELECT TXID,FROMADD,TOADD,VALUE,TYPE,BLOCKTIME,BLOCKNUM,VALID,CURTYPE from transactions where type='generate'"
        cmd2.ExecuteNonQuery()
        cmd2.CommandText = "insert into balances (address, cbalance, cbalancet) SELECT TOADD,SUM(VALUE),SUM(VALUE) from transactions_processed where curtype = 1 group by toadd"
        cmd2.ExecuteNonQuery()
        con2.Close()

        'go through simple transactions, check validity and apply to balances
        Try
            Dim con As New SqlClient.SqlConnection(sqlcon)
            Dim cmd As New SqlCommand()
            Dim sqlquery
            Dim returnval
            If debuglevel > 0 Then Console.WriteLine("DEBUG: SQL : " & sqlquery)
            cmd.Connection = con
            con.Open()
            sqlquery = "SELECT * FROM transactions order by 'ID'"
            cmd.CommandText = sqlquery
            Dim adptSQL As New SqlClient.SqlDataAdapter(cmd)
            Dim ds1 As New DataSet()
            adptSQL.Fill(ds1)

            With ds1.Tables(0)
                For rowNumber As Integer = 0 To .Rows.Count - 1
                    With .Rows(rowNumber)
                        If .Item(4) = "simple" Then
                            'get currency type
                            Dim curtype As Integer = .Item(8)
                            'get transaction amount
                            Dim txamount As Long = .Item(3)
                            'check senders input balance
                            If curtype = 1 Then sqlquery = "SELECT CBALANCE FROM balances where ADDRESS='" & .Item(1).ToString & "'"
                            If curtype = 2 Then sqlquery = "SELECT CBALANCET FROM balances where ADDRESS='" & .Item(1).ToString & "'"
                            cmd.CommandText = sqlquery
                            returnval = cmd.ExecuteScalar
                            'check if transaction amount is over senders balance
                            If returnval > txamount Then 'ok
                                cmd.CommandText = "INSERT INTO transactions_processed VALUES ('" & .Item(0).ToString & "','" & .Item(1).ToString & "','" & .Item(2).ToString & "'," & .Item(3).ToString & ",'" & .Item(4).ToString & "'," & .Item(5).ToString & "," & .Item(6).ToString & ",1," & .Item(8).ToString & ")"
                                returnval = cmd.ExecuteScalar
                                'subtract balances accordingly
                                If curtype = 1 Then cmd.CommandText = "UPDATE balances SET CBALANCE=CBALANCE-" & txamount & " where ADDRESS='" & .Item(1).ToString & "'"
                                If curtype = 2 Then cmd.CommandText = "UPDATE balances SET CBALANCET=CBALANCET-" & txamount & " where ADDRESS='" & .Item(1).ToString & "'"
                                returnval = cmd.ExecuteScalar
                                'add balances accordingly
                                'does address already in db?
                                sqlquery = "SELECT ADDRESS FROM balances where ADDRESS='" & .Item(2).ToString & "'"
                                cmd.CommandText = sqlquery
                                returnval = cmd.ExecuteScalar
                                If returnval = .Item(2).ToString Then
                                    If curtype = 1 Then cmd.CommandText = "UPDATE balances SET CBALANCE=CBALANCE+" & txamount & " where ADDRESS='" & .Item(2).ToString & "'"
                                    If curtype = 2 Then cmd.CommandText = "UPDATE balances SET CBALANCET=CBALANCET+" & txamount & " where ADDRESS='" & .Item(2).ToString & "'"
                                    returnval = cmd.ExecuteScalar
                                Else
                                    If curtype = 1 Then cmd.CommandText = "INSERT INTO balances (ADDRESS,CBALANCE,CBALANCET) VALUES ('" & .Item(2).ToString & "'," & txamount & ",0)"
                                    If curtype = 2 Then cmd.CommandText = "INSERT INTO balances (ADDRESS,CBALANCE,CBALANCET) VALUES ('" & .Item(2).ToString & "',0," & txamount & ")"
                                    returnval = cmd.ExecuteScalar
                                End If
                            Else 'transaction not valid
                                cmd.CommandText = "INSERT INTO transactions_processed VALUES ('" & .Item(0).ToString & "','" & .Item(1).ToString & "','" & .Item(2).ToString & "'," & .Item(3).ToString & ",'" & .Item(4).ToString & "'," & .Item(5).ToString & "," & .Item(6).ToString & ",0," & .Item(8).ToString & ")"
                                returnval = cmd.ExecuteScalar
                            End If
                        End If
                    End With
                Next
            End With
            con.Close()
        Catch e As Exception
            Console.WriteLine("ERROR: Connection to database threw an exception of: " & vbCrLf & e.Message.ToString & vbCrLf & "Exiting...")
            End
        End Try

    End Function

    Public Function SQLGetSingleVal(ByVal sqlquery)
        Try
            Dim con As New SqlClient.SqlConnection(sqlcon)
            Dim cmd As New SqlCommand()
            Dim returnval
            If debuglevel > 0 Then Console.WriteLine("DEBUG: SQL : " & sqlquery)
            cmd.Connection = con
            con.Open()
            cmd.CommandText = sqlquery
            returnval = cmd.ExecuteScalar
            If Not IsDBNull(returnval) Then Return returnval
            con.Close()
        Catch e As Exception
            'exception thrown connecting
            Console.WriteLine("ERROR: Connection to database threw an exception of: " & vbCrLf & e.Message.ToString & vbCrLf & "Exiting...")
            End
        End Try
    End Function

End Module

